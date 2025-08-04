import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from pandas import DataFrame


class Performance:
    def __init__(self, df, periods_in_year, trade_size=100, capital=10_000):
        """
        :param df: DataFrame with DatetimeIndex, 'signal' (0, 1 buy) and 'open' (price) columns
        """
        self.df = df.copy()
        self.trades_df: DataFrame = pd.DataFrame()
        self.periods_in_year = periods_in_year
        self.trade_size = trade_size
        self.capital = capital
        self._prepare_trade_performance()

        self.num_trades: int
        self.total_pnl: float
        self.capital_risked: float
        self.roi: float
        self.win_rate: float
        self.profit_factor_trade: float
        self.profit_factor_bar: float
        self.annualised_return_bar: float
        self.annualised_volatility_bar: float
        self.strategy_sharpe: float
        self.benchmark_sharpe: float
        self.max_drawdown: float
        self.gross_profit_trade: float
        self.gross_loss_trade: float
        self.gross_profit_bar: float
        self.gross_loss_bar: float

    def _prepare_trade_performance(self):
        records = []
        in_trade = False
        entry_time = None
        entry_price = None

        for t, row in self.df.iterrows():
            signal = int(row["signal"])
            price = float(row["open"])

            if not in_trade and signal == 1:
                entry_time = t
                entry_price = price
                in_trade = True

            elif in_trade and signal == 0:
                exit_time = t
                exit_price = price
                pnl = ((exit_price - entry_price) / entry_price) * self.trade_size
                records.append({
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "signal": 1,
                    "pnl": pnl,
                })
                in_trade = False
                entry_time = None
                entry_price = None

        # After the loop: close open trade if signal never went back to 0
        if in_trade:
            exit_time = self.df.index[-1]
            exit_price = float(self.df["open"].iloc[-1])
            pnl = ((exit_price - entry_price) / entry_price) * self.trade_size
            records.append({
                "entry_time": entry_time,
                "exit_time": exit_time,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "signal": 1,
                "pnl": pnl,
            })

        self.trades_df = pd.DataFrame.from_records(
            records,
            columns=["entry_time", "exit_time", "entry_price", "exit_price", "signal", "pnl"]
        )

    def _calculate_metrics_bar(self):
        self.df['log_return'] = np.log(self.df['open']).diff().shift(-1)
        self.df['log_pnl'] = self.df['signal'] * self.df['log_return']

        # Drop rows where log_return is NaN (would miss represent roi, PF, sharpe's, win rate)
        self.df.dropna(subset=['log_return'], inplace=True)

        self.profit_factor_bar = self._calc_profit_factor_bar()
        self.annualised_return_bar = self.df['log_pnl'].mean() * self.periods_in_year
        self.annualised_volatility_bar = self.df['log_pnl'].std() * np.sqrt(self.periods_in_year)
        self.strategy_sharpe = self._calc_strategy_sharpe()
        self.benchmark_sharpe = self._calc_benchmark_sharpe()
        self.max_drawdown = self._calc_max_drawdown()

    def _calculate_metrics_trade(self):
        df = self.trades_df

        self.num_trades = len(df)
        self.total_pnl = df["pnl"].sum()
        self.capital_risked = self.num_trades * self.trade_size
        self.roi = self.total_pnl / self.capital_risked if self.capital_risked != 0 else 0

        self.gross_profit_trade = df[df["pnl"] > 0]["pnl"].sum()
        self.gross_loss_trade = abs(df[df["pnl"] < 0]["pnl"].sum())
        self.profit_factor_trade = (self.gross_profit_trade / self.gross_loss_trade) if self.gross_loss_trade != 0 else np.inf

        self.wins = (df["pnl"] > 0).sum()
        self.losses = (df["pnl"] <= 0).sum()
        self.win_rate = self.wins / self.num_trades if self.num_trades > 0 else 0

    def _calc_profit_factor_bar(self):
        r = self.df['log_pnl']

        # Convert each log return to its % return
        pct_returns = np.exp(r) - 1
        pnl = pct_returns * self.trade_size

        # Sum dollar gains and losses
        self.gross_profit_bar = pnl[pnl > 0].sum()
        self.gross_loss_bar = pnl[pnl < 0].abs().sum()

        return self.gross_profit_bar / self.gross_loss_bar if self.gross_loss_bar != 0 else np.nan

    def _calc_strategy_sharpe(self):
        r = self.df['log_pnl']
        return r.mean() / r.std() * np.sqrt(self.periods_in_year) if r.std() != 0 else np.nan

    def _calc_benchmark_sharpe(self):
        r = self.df['log_return']
        return r.mean() / r.std() * np.sqrt(self.periods_in_year) if r.std() != 0 else np.nan

    def _calc_max_drawdown(self):
        cum_pnl = (np.exp(self.df['log_pnl'].cumsum()) - 1) * self.trade_size
        peak = cum_pnl.cummax()
        drawdown = peak - cum_pnl
        return drawdown.max()

    def plot_cumulative_pnl(self):
        if self.trades_df.empty:
            print("No trades to plot.")
            return

        df = self.trades_df.sort_values('exit_time').copy()
        df['cumulative_pnl'] = df['pnl'].cumsum()

        x_pnl = df['exit_time'].values
        y_pnl = df['cumulative_pnl'].values

        plt.figure(figsize=(12, 6))

        start_idx = 0
        for i in range(1, len(y_pnl)):
            if (y_pnl[i - 1] >= 0 > y_pnl[i]) or (y_pnl[i - 1] < 0 <= y_pnl[i]):
                color = 'blue' if y_pnl[i - 1] >= 0 else 'red'
                plt.plot(x_pnl[start_idx:i], y_pnl[start_idx:i], color=color)
                start_idx = i
        color = 'blue' if y_pnl[-1] >= 0 else 'red'
        plt.plot(x_pnl[start_idx:], y_pnl[start_idx:], color=color, label='Cumulative PnL')

        plt.xlabel('Time')
        plt.ylabel('Cumulative PnL')
        plt.title('Cumulative PnL Over Time')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()

    def evaluate(self):
        self._calculate_metrics_trade()
        self._calculate_metrics_bar()
        return pd.Series({
            'pnl': round(self.total_pnl, 2),
            'trade_count': self.num_trades,
            'trade_size': self.trade_size,
            'capital': self.capital,
            'capital_risked': self.num_trades * self.trade_size,
            'roi': round(self.roi, 4),
            'win_rate': round(self.win_rate, 4),
            'profit_factor_trade': round(self.profit_factor_trade, 4),
            'profit_factor_bar': round(self.profit_factor_bar, 4),
            'annualised_return_bar': round(self.annualised_return_bar, 4),
            'annualised_volatility_bar': round(self.annualised_volatility_bar, 4),
            'strategy_sharpe': round(self.strategy_sharpe, 4),
            'benchmark_sharpe': round(self.benchmark_sharpe, 4),
            'max_drawdown': round(self.max_drawdown, 4),
            'gross_profit_trade': round(self.gross_profit_trade, 4),
            'gross_loss_trade': round(self.gross_loss_trade, 4),
            'gross_profit_bar': round(self.gross_profit_bar, 4),
            'gross_loss_bar': round(self.gross_loss_bar, 4),
        })
