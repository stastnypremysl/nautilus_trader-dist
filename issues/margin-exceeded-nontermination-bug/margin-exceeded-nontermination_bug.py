#!/usr/bin/env python3

from nautilus_trader.config import ImportableStrategyConfig, LoggingConfig
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from nautilus_trader.model import TradeTick, InstrumentId, Quantity, QuoteTick, InstrumentId, Position
from nautilus_trader.model.enums import OrderSide, PositionSide, PriceType
from nautilus_trader.model.events import PositionOpened, PositionChanged, PositionClosed, OrderAccepted, OrderFilled

from nautilus_trader.backtest.results import BacktestResult
from nautilus_trader.backtest.node import BacktestDataConfig, BacktestEngineConfig, BacktestNode, BacktestRunConfig, BacktestVenueConfig

from nautilus_trader.core.message import Event
from nautilus_trader.core.rust.common import LogColor
from nautilus_trader.core.rust.model import TriggerType, TimeInForce

from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence
from nautilus_trader.trading.strategy import Strategy, StrategyConfig



catalog = ParquetDataCatalog("/home/p/p-dev/nautilus/catalog")
instrument_id = InstrumentId.from_str("APTUSDT-PERP.BINANCE")


venue = BacktestVenueConfig(
    name="BINANCE",
    oms_type="NETTING",
    account_type="MARGIN",
    base_currency="USDT",
    starting_balances=["100 USDT"],
    default_leverage=5,
    leverages={"APTUSDT-PERP.BINANCE": 5}
)


data = BacktestDataConfig(
    catalog_path=str(catalog.path),
    data_cls=TradeTick,
    instrument_ids=[instrument_id],
    start_time="2024-12-10",
    end_time="2025-01-10"
)

engine = BacktestEngineConfig(
    strategies=[
        ImportableStrategyConfig(
            strategy_path="__main__:MACDStrategy",
            config_path="__main__:MACDConfig",
            config={
              "instrument_id": instrument_id,
              "fast_period": 12,
              "slow_period": 26,
            },
        )
    ],
    logging=LoggingConfig(log_level="INFO"),
)



class MACDConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    fast_period: int = 12
    slow_period: int = 26
    trade_size: int = 10
    entry_threshold: float = 0.00010


class MACDStrategy(Strategy):
    def __init__(self, config: MACDConfig):
        super().__init__(config=config)
        self.log.info("Initialization of MACDStrategy")
        # Our "trading signal"
        self.macd = MovingAverageConvergenceDivergence(
            fast_period=config.fast_period, slow_period=config.slow_period, price_type=PriceType.MID
        )

        # Convenience
        self.position: Position | None = None

    
    def on_start(self):
        self.log.info("Start of MACDStrategy")
        
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self.account = self.portfolio.account(self.instrument.venue)
        self.trade_size = self.instrument.make_qty(self.config.trade_size)

        self.subscribe_trade_ticks(instrument_id=self.config.instrument_id)


    def on_stop(self):
        self.log.info("Stop of MACDStrategy")
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_trade_ticks(instrument_id=self.config.instrument_id)

    

    def on_trade_tick(self, tick: TradeTick):
        # You can register indicators to receive quote tick updates automatically,
        # here we manually update the indicator to demonstrate the flexibility available
        self.macd.handle_trade_tick(tick)

        if not self.macd.initialized:
            return  # Wait for indicator to warm up

        # self._log.info(f"{self.macd.value=}:%5d")
        self.check_for_entry(tick)
        self.check_for_exit()


    def check_for_entry(self, tick: TradeTick):
        quantity = self.instrument.make_qty(
            self.config.trade_size
        )
        
        # If MACD line is above our entry threshold, we should be LONG
        if self.macd.value > self.config.entry_threshold:
            if self.position and self.position.side == PositionSide.LONG:
                return  # Already LONG

            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=quantity,
            )
            self.position_open_price = tick.price
            self.submit_order(order)
            
        # If MACD line is below our entry threshold, we should be SHORT
        elif self.macd.value < -self.config.entry_threshold:
            if self.position and self.position.side == PositionSide.SHORT:
                return  # Already SHORT
            
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.SELL,
                quantity=quantity,
            )
            self.position_open_price = tick.price
            self.submit_order(order)
            

    def check_for_exit(self):
        # If MACD line is above zero then exit if we are SHORT
        if self.macd.value >= 0.0:
            if self.position and self.position.side == PositionSide.SHORT:
                self.log.info(f"Invoking self.close_position({self.position})", LogColor.MAGENTA)
                self.close_position(self.position)

        # If MACD line is below zero then exit if we are LONG
        else:
            if self.position and self.position.side == PositionSide.LONG:
                self.log.info(f"Invoking self.close_position({self.position})", LogColor.MAGENTA)
                self.close_position(self.position)

    
    def on_dispose(self):
        pass  # Do nothing else



config = BacktestRunConfig(
    engine=engine,
    venues=[venue],
    data=[data],
    chunk_size=1024*32
)



node = BacktestNode(configs=[config])

results: list[BacktestResult] = node.run()