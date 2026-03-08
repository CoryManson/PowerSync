"""Sigenergy inverter controller via Modbus TCP.

Supports Sigenergy hybrid inverter systems for DC solar curtailment.
Uses the plant-level PV power limit and active power percentage registers.

Reference: https://github.com/TypQxQ/Sigenergy-Local-Modbus
"""
import asyncio
import logging
from typing import Optional

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
import pymodbus

from .base import InverterController, InverterState, InverterStatus

_LOGGER = logging.getLogger(__name__)

# pymodbus 3.9+ changed 'slave' parameter to 'device_id'
try:
    _pymodbus_version = tuple(int(x) for x in pymodbus.__version__.split(".")[:2])
    _SLAVE_PARAM = "device_id" if _pymodbus_version >= (3, 9) else "slave"
except Exception:
    _SLAVE_PARAM = "slave"  # Fallback to older parameter name


class SigenergyController(InverterController):
    """Controller for Sigenergy hybrid inverter systems via Modbus TCP.

    Uses Modbus TCP to communicate directly with the Sigenergy system
    for DC solar curtailment control.
    """

    # Modbus register addresses - use FULL addresses (pymodbus handles protocol details)
    # Reference: https://github.com/TypQxQ/Sigenergy-Local-Modbus

    # === PLANT-LEVEL REGISTERS (slave ID 247) ===
    # Holding registers (read/write)
    REG_PV_MAX_POWER_LIMIT = 40036        # PV max power limit (U32, gain 1000, kW)
    REG_ACTIVE_POWER_PCT_TARGET = 40005   # Active power % target (S16, gain 100)
    REG_ACTIVE_POWER_FIXED_TARGET = 40001 # Active power fixed target (S32, gain 1000, kW)
    REG_GRID_EXPORT_LIMIT = 40038         # Grid Point Maximum export limitation (U32, gain 1000)
    REG_PCS_EXPORT_LIMIT = 40042          # PCS maximum export limitation (U32, gain 1000)
    REG_ESS_MAX_CHARGE_LIMIT = 40032      # ESS max charging (U32, gain 1000, kW)
    REG_ESS_MAX_DISCHARGE_LIMIT = 40034   # ESS max discharging (U32, gain 1000, kW)
    REG_REMOTE_EMS_ENABLE = 40029         # Remote EMS enable (U16: 0=disabled, 1=enabled)
    REG_REMOTE_EMS_CONTROL_MODE = 40031   # Remote EMS control mode (U16)
    REG_ESS_BACKUP_SOC = 40046            # ESS backup SOC (U16, gain 10, %)
    REG_ESS_CHARGE_CUTOFF_SOC = 40047     # ESS charge cut-off SOC (U16, gain 10, %)
    REG_ESS_DISCHARGE_CUTOFF_SOC = 40048  # ESS discharge cut-off SOC (U16, gain 10, %)

    # Input registers (read-only) - Real-time power
    REG_PV_POWER = 30035                  # PV power (S32, gain 1000, kW)
    REG_ACTIVE_POWER = 30031              # Active power (S32, gain 1000, kW)
    REG_ESS_SOC = 30014                   # Battery SOC (U16, gain 10, %)
    REG_ESS_POWER = 30037                 # Battery power (S32, gain 1000, kW)
    REG_RUNNING_STATE = 30051             # Plant running state (U16)
    REG_GRID_SENSOR_POWER = 30005         # Grid sensor active power (S32, gain 1000, kW)
    REG_EMS_WORK_MODE = 30003             # EMS work mode (U16)

    # Input registers (read-only) - Battery health
    REG_ESS_RATED_CAPACITY = 30083        # ESS rated energy capacity (U32, gain 100, kWh)
    REG_ESS_SOH = 30087                   # Battery State of Health (U16, gain 10, %)
    REG_ESS_RATED_CHARGE_POWER = 30079    # ESS rated charge power (U32, gain 1000, kW)
    REG_ESS_RATED_DISCHARGE_POWER = 30081 # ESS rated discharge power (U32, gain 1000, kW)

    # Input registers (read-only) - Energy totals (U64 = 4 registers, gain 100)
    REG_ACCUMULATED_PV_ENERGY = 30088     # Total PV generation (U64, gain 100, kWh)
    REG_DAILY_CONSUMED_ENERGY = 30092     # Daily load consumption (U32, gain 100, kWh)
    REG_ACCUMULATED_CONSUMED_ENERGY = 30094  # Total load consumption (U64, gain 100, kWh)
    REG_ACCUMULATED_BATTERY_CHARGE = 30200   # Total battery charged (U64, gain 100, kWh)
    REG_ACCUMULATED_BATTERY_DISCHARGE = 30204  # Total battery discharged (U64, gain 100, kWh)
    REG_ACCUMULATED_GRID_IMPORT = 30216   # Total grid import (U64, gain 100, kWh)
    REG_ACCUMULATED_GRID_EXPORT = 30220   # Total grid export (U64, gain 100, kWh)

    # === INVERTER-LEVEL REGISTERS (slave ID 1) ===
    # Fallback if plant registers don't work
    REG_INV_SOC = 30601                   # Inverter battery SOC (U16, gain 10, %)
    REG_INV_SOH = 30602                   # Inverter battery SOH (U16, gain 10, %)
    REG_INV_ACTIVE_POWER = 30587          # Inverter active power (S32, gain 1000, kW)
    REG_INV_ESS_POWER = 30599             # Inverter battery power (S32, gain 1000, kW)
    REG_INV_PV_POWER = 31035              # Inverter PV power (S32, gain 1000, kW)

    # Constants
    GAIN_POWER = 1000  # kW → scaled value (multiply to write, divide to read)
    GAIN_PERCENT = 100  # % → scaled value
    GAIN_SOC = 10      # % → scaled value
    GAIN_ENERGY = 100  # kWh → scaled value for energy registers

    # Curtailment values
    # Use self-consumption mode with explicit zero-export limit rather than PV shutdown
    EXPORT_LIMIT_ZERO = 0         # Zero grid export target
    EXPORT_LIMIT_UNLIMITED = 0xFFFFFFFE  # Unlimited export (normal operation)
    EXPORT_LIMIT_INVALID = 0xFFFFFFFF    # Register invalid per Sigenergy Modbus docs
    PV_POWER_LIMIT_ZERO = 0       # Set PV limit to 0 kW (full shutdown - not used)
    ACTIVE_POWER_PCT_ZERO = 0     # 0% active power

    # Default Modbus settings
    # Sigenergy uses different slave IDs for different register levels:
    # - Plant-level registers (30001-30099): Slave ID 247
    # - Inverter-level registers (30500+): Slave ID 1 (or specific inverter address)
    DEFAULT_PORT = 502
    DEFAULT_SLAVE_ID = 247  # Plant address - will auto-switch to 1 for inverter registers
    DEFAULT_INVERTER_SLAVE_ID = 1  # Default inverter address
    TIMEOUT_SECONDS = 10.0

    # Remote EMS control modes (per Sigenergy Modbus protocol)
    REMOTE_EMS_MODE_PCS_REMOTE_CONTROL = 0
    REMOTE_EMS_MODE_STANDBY = 1
    REMOTE_EMS_MODE_MAX_SELF_CONSUMPTION = 2
    REMOTE_EMS_MODE_COMMAND_CHARGING_GRID_FIRST = 3
    REMOTE_EMS_MODE_COMMAND_CHARGING_PV_FIRST = 4
    REMOTE_EMS_MODE_COMMAND_DISCHARGING_PV_FIRST = 5
    REMOTE_EMS_MODE_COMMAND_DISCHARGING_ESS_FIRST = 6

    def __init__(
        self,
        host: str,
        port: int = 502,
        slave_id: int = 1,
        max_export_limit_kw: Optional[float] = None,
        read_only: bool = False,
        model: Optional[str] = None,
    ):
        """Initialize Sigenergy controller.

        Args:
            host: IP address of Sigenergy system
            port: Modbus TCP port (default: 502)
            slave_id: Modbus slave ID (default: 1)
            max_export_limit_kw: Optional hard safety cap for grid export limit
            read_only: If True, block all Modbus write operations
            model: Sigenergy model (optional)
        """
        super().__init__(host, port, slave_id, model)
        self._client: Optional[AsyncModbusTcpClient] = None
        self._lock = asyncio.Lock()
        self._original_pv_limit: Optional[int] = None  # Store original limit for restore
        self._use_inverter_registers: Optional[bool] = None  # None=unknown, True=inverter, False=plant
        # For AC Charger setups: AC Charger is slave 1, inverter is slave 2
        # Use user-configured slave_id for inverter registers instead of hardcoded default
        # This allows users with AC Chargers to specify slave 2 and have it work correctly
        self._inverter_slave_id = slave_id if slave_id != self.DEFAULT_SLAVE_ID else self.DEFAULT_INVERTER_SLAVE_ID
        try:
            if max_export_limit_kw in (None, ""):
                self._configured_max_export_limit_kw = None
            else:
                value = float(max_export_limit_kw)
                self._configured_max_export_limit_kw = value if value >= 0 else None
        except (TypeError, ValueError):
            self._configured_max_export_limit_kw = None
        self._read_only = bool(read_only)

    async def connect(self) -> bool:
        """Connect to the Sigenergy system via Modbus TCP."""
        async with self._lock:
            try:
                if self._client and self._client.connected:
                    return True

                self._client = AsyncModbusTcpClient(
                    host=self.host,
                    port=self.port,
                    timeout=self.TIMEOUT_SECONDS,
                )

                connected = await self._client.connect()
                if connected:
                    self._connected = True
                    _LOGGER.info(
                        f"Connected to Sigenergy system at {self.host}:{self.port} "
                        f"(plant slave={self.slave_id}, inverter slave={self._inverter_slave_id})"
                    )
                else:
                    _LOGGER.error(f"Failed to connect to Sigenergy at {self.host}:{self.port}")

                return connected

            except Exception as e:
                _LOGGER.error(f"Error connecting to Sigenergy: {e}")
                self._connected = False
                return False

    async def disconnect(self) -> None:
        """Disconnect from the Sigenergy system."""
        async with self._lock:
            if self._client:
                self._client.close()
                self._client = None
            self._connected = False
            _LOGGER.debug(f"Disconnected from Sigenergy at {self.host}")

    async def _write_holding_registers(self, address: int, values: list[int], slave_id: Optional[int] = None) -> bool:
        """Write values to holding registers.

        Args:
            address: Starting register address (0-indexed)
            values: List of values to write
            slave_id: Optional slave ID override (default: DEFAULT_SLAVE_ID=247 for plant registers)

        Returns:
            True if write successful
        """
        if self._read_only:
            _LOGGER.warning(
                "Sigenergy write blocked by read-only mode: register=%s values=%s",
                address,
                values,
            )
            return False

        if not self._client or not self._client.connected:
            if not await self.connect():
                return False

        effective_slave = slave_id if slave_id is not None else self.DEFAULT_SLAVE_ID

        try:
            result = await self._client.write_registers(
                address=address,
                values=values,
                **{_SLAVE_PARAM: effective_slave},
            )

            if result.isError():
                _LOGGER.error(f"Modbus write error at register {address}: {result}")
                return False

            _LOGGER.debug(f"Successfully wrote {values} to register {address}")
            return True

        except ModbusException as e:
            _LOGGER.error(f"Modbus exception writing to register {address}: {e}")
            return False
        except Exception as e:
            _LOGGER.error(f"Error writing to register {address}: {e}")
            return False

    async def _read_holding_registers(self, address: int, count: int = 1, slave_id: Optional[int] = None) -> Optional[list]:
        """Read values from holding registers.

        Args:
            address: Starting register address (0-indexed)
            count: Number of registers to read
            slave_id: Optional slave ID override (default: DEFAULT_SLAVE_ID=247 for plant registers)

        Returns:
            List of register values or None on error
        """
        if not self._client or not self._client.connected:
            if not await self.connect():
                return None

        effective_slave = slave_id if slave_id is not None else self.DEFAULT_SLAVE_ID

        try:
            result = await self._client.read_holding_registers(
                address=address,
                count=count,
                **{_SLAVE_PARAM: effective_slave},
            )

            if result.isError():
                _LOGGER.debug(f"Modbus read error at holding register {address}: {result}")
                return None

            return result.registers

        except ModbusException as e:
            _LOGGER.debug(f"Modbus exception reading holding register {address}: {e}")
            return None
        except Exception as e:
            _LOGGER.debug(f"Error reading holding register {address}: {e}")
            return None

    async def _read_input_registers(self, address: int, count: int = 1, slave_id: Optional[int] = None) -> Optional[list]:
        """Read values from input registers.

        Args:
            address: Starting register address (0-indexed)
            count: Number of registers to read
            slave_id: Optional slave ID override (default: self.slave_id)

        Returns:
            List of register values or None on error
        """
        if not self._client or not self._client.connected:
            if not await self.connect():
                return None

        effective_slave = slave_id if slave_id is not None else self.slave_id

        try:
            result = await self._client.read_input_registers(
                address=address,
                count=count,
                **{_SLAVE_PARAM: effective_slave},
            )

            if result.isError():
                _LOGGER.debug(f"Modbus read error at input register {address} [slave={effective_slave}]: {result}")
                return None

            return result.registers

        except ModbusException as e:
            _LOGGER.debug(f"Modbus exception reading input register {address} [slave={effective_slave}]: {e}")
            return None
        except Exception as e:
            _LOGGER.debug(f"Error reading input register {address} [slave={effective_slave}]: {e}")
            return None

    def _to_signed32(self, high: int, low: int) -> int:
        """Convert two unsigned 16-bit registers to signed 32-bit."""
        value = (high << 16) | low
        if value >= 0x80000000:
            value -= 0x100000000
        return value

    def _to_unsigned32(self, high: int, low: int) -> int:
        """Convert two unsigned 16-bit registers to unsigned 32-bit."""
        return (high << 16) | low

    def _from_unsigned32(self, value: int) -> list[int]:
        """Convert unsigned 32-bit to two 16-bit registers [high, low]."""
        high = (value >> 16) & 0xFFFF
        low = value & 0xFFFF
        return [high, low]

    def _to_unsigned64(self, regs: list[int]) -> int:
        """Convert four unsigned 16-bit registers to unsigned 64-bit.

        Register order: [high_high, high_low, low_high, low_low]
        """
        if len(regs) < 4:
            return 0
        return (regs[0] << 48) | (regs[1] << 32) | (regs[2] << 16) | regs[3]

    async def _get_current_pv_limit(self) -> Optional[int]:
        """Read current PV power limit."""
        regs = await self._read_holding_registers(self.REG_PV_MAX_POWER_LIMIT, 2)
        if regs and len(regs) >= 2:
            return self._to_unsigned32(regs[0], regs[1])
        return None

    async def _get_current_export_limit(self) -> Optional[int]:
        """Read current grid export limit."""
        regs = await self._read_holding_registers(self.REG_GRID_EXPORT_LIMIT, 2)
        if regs and len(regs) >= 2:
            return self._to_unsigned32(regs[0], regs[1])
        return None

    async def _get_current_pcs_export_limit(self) -> Optional[int]:
        """Read current PCS export limit."""
        regs = await self._read_holding_registers(self.REG_PCS_EXPORT_LIMIT, 2)
        if regs and len(regs) >= 2:
            return self._to_unsigned32(regs[0], regs[1])
        return None

    async def _get_ess_rated_charge_discharge_power_kw(self) -> tuple[Optional[float], Optional[float]]:
        """Read ESS rated charge/discharge powers in kW (input registers)."""
        charge_kw: Optional[float] = None
        discharge_kw: Optional[float] = None

        charge_regs = await self._read_input_registers(self.REG_ESS_RATED_CHARGE_POWER, 2)
        if charge_regs and len(charge_regs) >= 2:
            charge_val = self._to_unsigned32(charge_regs[0], charge_regs[1])
            if charge_val not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID):
                charge_kw = charge_val / self.GAIN_POWER

        discharge_regs = await self._read_input_registers(self.REG_ESS_RATED_DISCHARGE_POWER, 2)
        if discharge_regs and len(discharge_regs) >= 2:
            discharge_val = self._to_unsigned32(discharge_regs[0], discharge_regs[1])
            if discharge_val not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID):
                discharge_kw = discharge_val / self.GAIN_POWER

        return charge_kw, discharge_kw

    async def _get_ess_max_charge_discharge_limit_kw(self) -> tuple[Optional[float], Optional[float]]:
        """Read current ESS max charge/discharge limits in kW (holding registers)."""
        charge_kw: Optional[float] = None
        discharge_kw: Optional[float] = None

        charge_regs = await self._read_holding_registers(self.REG_ESS_MAX_CHARGE_LIMIT, 2)
        if charge_regs and len(charge_regs) >= 2:
            charge_val = self._to_unsigned32(charge_regs[0], charge_regs[1])
            if charge_val not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID):
                charge_kw = charge_val / self.GAIN_POWER

        discharge_regs = await self._read_holding_registers(self.REG_ESS_MAX_DISCHARGE_LIMIT, 2)
        if discharge_regs and len(discharge_regs) >= 2:
            discharge_val = self._to_unsigned32(discharge_regs[0], discharge_regs[1])
            if discharge_val not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID):
                discharge_kw = discharge_val / self.GAIN_POWER

        return charge_kw, discharge_kw

    async def _get_effective_export_safety_cap_kw(self) -> Optional[float]:
        """Get effective export safety cap in kW.

        Priority:
        1) Configured cap passed by integration
        2) ESS rated charge/discharge powers (auto cap uses the lower value)
        3) ESS max charge/discharge limits (lower value)
        4) PCS export limitation register (40042) when valid
        5) Grid export limitation register (40038) when valid
        """
        if self._configured_max_export_limit_kw is not None and self._configured_max_export_limit_kw >= 0:
            return self._configured_max_export_limit_kw

        rated_charge_kw, rated_discharge_kw = await self._get_ess_rated_charge_discharge_power_kw()
        rated_values = [v for v in (rated_charge_kw, rated_discharge_kw) if v is not None and v >= 0]
        if rated_values:
            return min(rated_values)

        max_charge_kw, max_discharge_kw = await self._get_ess_max_charge_discharge_limit_kw()
        max_values = [v for v in (max_charge_kw, max_discharge_kw) if v is not None and v >= 0]
        if max_values:
            return min(max_values)

        pcs_limit = await self._get_current_pcs_export_limit()
        if pcs_limit is not None and pcs_limit not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID):
            return pcs_limit / self.GAIN_POWER

        grid_limit = await self._get_current_export_limit()
        if grid_limit is not None and grid_limit not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID):
            return grid_limit / self.GAIN_POWER

        return None

    async def resolve_export_safety_cap_kw(self) -> Optional[float]:
        """Public helper to resolve current effective export safety cap in kW."""
        return await self._get_effective_export_safety_cap_kw()

    async def curtail(
        self,
        home_load_w: Optional[float] = None,
        rated_capacity_w: Optional[float] = None,
    ) -> bool:
        """Curtail export using self-consumption mode with zero grid export.

        For Sigenergy, the safest curtailment behavior is:
        1) Keep/enter Remote EMS mode
        2) Set control mode to Maximum self-consumption (mode 2)
        3) Set grid export limit to 0 kW

        This avoids load-following behavior that can create import/export
        oscillations during curtailment windows.

        Args:
            home_load_w: Unused for Sigenergy curtailment strategy
            rated_capacity_w: Unused for interface compatibility

        Returns:
            True if curtailment successful
        """
        try:
            if not await self.connect():
                _LOGGER.error("Cannot curtail: failed to connect to Sigenergy")
                return False

            # Store original export limit if not already stored
            if self._original_pv_limit is None:
                self._original_pv_limit = await self._get_current_export_limit()
                if self._original_pv_limit is not None:
                    limit_str = f"{self._original_pv_limit / self.GAIN_POWER} kW" if self._original_pv_limit < self.EXPORT_LIMIT_UNLIMITED else "unlimited"
                    _LOGGER.info(f"Stored original export limit: {limit_str}")

            _LOGGER.info(
                "Curtailing Sigenergy at %s (self-consumption mode + zero export)",
                self.host,
            )

            # 1) Ensure Remote EMS is enabled
            ems_result = await self._set_remote_ems_enabled(True)
            if not ems_result:
                _LOGGER.error("Failed to enable Remote EMS for curtailment")
                return False

            # 2) Force max self-consumption mode
            mode_result = await self._set_remote_ems_control_mode(
                self.REMOTE_EMS_MODE_MAX_SELF_CONSUMPTION
            )
            if not mode_result:
                _LOGGER.error("Failed to set Remote EMS mode to max self-consumption for curtailment")
                return False

            # 3) Zero export limit
            success = await self.set_export_limit(0)

            if success:
                _LOGGER.info("Successfully set Sigenergy self-consumption + zero export curtailment")
                # Brief delay then verify
                await asyncio.sleep(1)
                state = await self.get_status()
                if state.is_curtailed:
                    _LOGGER.info("Curtailment verified - self-consumption zero-export active")
                else:
                    _LOGGER.warning("Curtailment command sent but verification pending")
            else:
                _LOGGER.error(f"Failed to curtail Sigenergy at {self.host}")

            return success

        except Exception as e:
            _LOGGER.error(f"Error curtailing Sigenergy: {e}")
            return False

    async def restore(self) -> bool:
        """Restore normal export operation.

        Restores grid export limit to the original finite value or safety cap.

        Returns:
            True if restore successful
        """
        _LOGGER.info(f"Restoring Sigenergy export at {self.host}")

        try:
            if not await self.connect():
                _LOGGER.error("Cannot restore: failed to connect to Sigenergy")
                return False

            # Restore to original value when finite, otherwise fall back to safety cap.
            restore_kw: Optional[float] = None
            if (
                self._original_pv_limit is not None
                and self._original_pv_limit not in (self.EXPORT_LIMIT_UNLIMITED, self.EXPORT_LIMIT_INVALID)
            ):
                restore_kw = self._original_pv_limit / self.GAIN_POWER
            else:
                restore_kw = await self._get_effective_export_safety_cap_kw()

            if restore_kw is None:
                _LOGGER.error("Cannot restore Sigenergy export: no finite safety cap available")
                return False

            _LOGGER.info(f"Restoring export limit to safety value: {restore_kw:.2f} kW")
            success = await self.set_export_limit(restore_kw)

            if success:
                _LOGGER.info(f"Successfully restored Sigenergy export at {self.host}")
                # Clear stored limit after successful restore
                self._original_pv_limit = None
                # Brief delay then verify
                await asyncio.sleep(1)
                state = await self.get_status()
                if not state.is_curtailed:
                    _LOGGER.info("Restore verified - normal export resumed")
                else:
                    _LOGGER.warning("Restore command sent but may take time to resume")
            else:
                _LOGGER.error(f"Failed to restore Sigenergy at {self.host}")

            return success

        except Exception as e:
            _LOGGER.error(f"Error restoring Sigenergy: {e}")
            return False

    async def _read_plant_registers(self) -> dict:
        """Try to read plant-level registers."""
        attrs = {}
        success_count = 0

        # Read PV power (S32, 2 registers)
        pv_power_regs = await self._read_input_registers(self.REG_PV_POWER, 2)
        if pv_power_regs and len(pv_power_regs) >= 2:
            pv_power_kw = self._to_signed32(pv_power_regs[0], pv_power_regs[1]) / self.GAIN_POWER
            attrs["pv_power_kw"] = round(pv_power_kw, 2)
            attrs["pv_power_w"] = pv_power_kw * 1000
            success_count += 1

        # Read battery SOC (U16)
        soc_regs = await self._read_input_registers(self.REG_ESS_SOC, 1)
        if soc_regs:
            attrs["battery_soc"] = round(soc_regs[0] / self.GAIN_SOC, 1)
            success_count += 1

        # Read grid sensor power (S32, 2 registers)
        grid_power_regs = await self._read_input_registers(self.REG_GRID_SENSOR_POWER, 2)
        if grid_power_regs and len(grid_power_regs) >= 2:
            grid_power_kw = self._to_signed32(grid_power_regs[0], grid_power_regs[1]) / self.GAIN_POWER
            attrs["grid_power_kw"] = round(grid_power_kw, 2)
            success_count += 1

        # Read battery power (S32, 2 registers)
        ess_power_regs = await self._read_input_registers(self.REG_ESS_POWER, 2)
        if ess_power_regs and len(ess_power_regs) >= 2:
            ess_power_kw = self._to_signed32(ess_power_regs[0], ess_power_regs[1]) / self.GAIN_POWER
            attrs["battery_power_kw"] = round(ess_power_kw, 2)
            success_count += 1

        # Read battery SOH (U16, gain 10)
        soh_regs = await self._read_input_registers(self.REG_ESS_SOH, 1)
        if soh_regs:
            attrs["battery_soh"] = round(soh_regs[0] / self.GAIN_SOC, 1)

        # Read rated capacity (U32, gain 100, kWh)
        capacity_regs = await self._read_input_registers(self.REG_ESS_RATED_CAPACITY, 2)
        if capacity_regs and len(capacity_regs) >= 2:
            capacity_kwh = self._to_unsigned32(capacity_regs[0], capacity_regs[1]) / self.GAIN_ENERGY
            attrs["battery_capacity_kwh"] = round(capacity_kwh, 2)

        attrs["_success_count"] = success_count
        attrs["_register_level"] = "plant"
        return attrs

    async def _read_inverter_registers(self) -> dict:
        """Try to read inverter-level registers (fallback).

        Uses inverter slave ID (default: 1) instead of plant slave ID (247).
        """
        attrs = {}
        success_count = 0
        inv_slave = self._inverter_slave_id
        _LOGGER.debug(f"Reading inverter registers with slave ID {inv_slave}")

        # Read inverter PV power (S32, 2 registers)
        pv_power_regs = await self._read_input_registers(self.REG_INV_PV_POWER, 2, slave_id=inv_slave)
        if pv_power_regs and len(pv_power_regs) >= 2:
            pv_power_kw = self._to_signed32(pv_power_regs[0], pv_power_regs[1]) / self.GAIN_POWER
            attrs["pv_power_kw"] = round(pv_power_kw, 2)
            attrs["pv_power_w"] = pv_power_kw * 1000
            success_count += 1

        # Read inverter battery SOC (U16)
        soc_regs = await self._read_input_registers(self.REG_INV_SOC, 1, slave_id=inv_slave)
        if soc_regs:
            attrs["battery_soc"] = round(soc_regs[0] / self.GAIN_SOC, 1)
            success_count += 1

        # Read inverter active power (S32, 2 registers) - use as grid proxy
        active_power_regs = await self._read_input_registers(self.REG_INV_ACTIVE_POWER, 2, slave_id=inv_slave)
        if active_power_regs and len(active_power_regs) >= 2:
            active_power_kw = self._to_signed32(active_power_regs[0], active_power_regs[1]) / self.GAIN_POWER
            attrs["active_power_kw"] = round(active_power_kw, 2)
            success_count += 1

        # Read inverter battery power (S32, 2 registers)
        ess_power_regs = await self._read_input_registers(self.REG_INV_ESS_POWER, 2, slave_id=inv_slave)
        if ess_power_regs and len(ess_power_regs) >= 2:
            ess_power_kw = self._to_signed32(ess_power_regs[0], ess_power_regs[1]) / self.GAIN_POWER
            attrs["battery_power_kw"] = round(ess_power_kw, 2)
            success_count += 1

        attrs["_success_count"] = success_count
        attrs["_register_level"] = "inverter"
        attrs["_inverter_slave_id"] = inv_slave
        return attrs

    async def get_energy_summary(self) -> dict:
        """Read accumulated energy totals from Modbus registers.

        Returns a dict with lifetime and daily energy statistics in kWh.
        All U64 values use gain factor 100.
        """
        energy = {}

        try:
            if not await self.connect():
                return {"error": "Failed to connect to Sigenergy"}

            # Total PV generation (U64, 4 registers, gain 100)
            pv_regs = await self._read_input_registers(self.REG_ACCUMULATED_PV_ENERGY, 4)
            if pv_regs and len(pv_regs) >= 4:
                energy["total_pv_energy_kwh"] = round(self._to_unsigned64(pv_regs) / self.GAIN_ENERGY, 2)

            # Daily load consumption (U32, 2 registers, gain 100)
            daily_load_regs = await self._read_input_registers(self.REG_DAILY_CONSUMED_ENERGY, 2)
            if daily_load_regs and len(daily_load_regs) >= 2:
                energy["daily_load_energy_kwh"] = round(self._to_unsigned32(daily_load_regs[0], daily_load_regs[1]) / self.GAIN_ENERGY, 2)

            # Total load consumption (U64, 4 registers, gain 100)
            total_load_regs = await self._read_input_registers(self.REG_ACCUMULATED_CONSUMED_ENERGY, 4)
            if total_load_regs and len(total_load_regs) >= 4:
                energy["total_load_energy_kwh"] = round(self._to_unsigned64(total_load_regs) / self.GAIN_ENERGY, 2)

            # Total battery charged (U64, 4 registers, gain 100)
            charge_regs = await self._read_input_registers(self.REG_ACCUMULATED_BATTERY_CHARGE, 4)
            if charge_regs and len(charge_regs) >= 4:
                energy["total_battery_charged_kwh"] = round(self._to_unsigned64(charge_regs) / self.GAIN_ENERGY, 2)

            # Total battery discharged (U64, 4 registers, gain 100)
            discharge_regs = await self._read_input_registers(self.REG_ACCUMULATED_BATTERY_DISCHARGE, 4)
            if discharge_regs and len(discharge_regs) >= 4:
                energy["total_battery_discharged_kwh"] = round(self._to_unsigned64(discharge_regs) / self.GAIN_ENERGY, 2)

            # Total grid import (U64, 4 registers, gain 100)
            import_regs = await self._read_input_registers(self.REG_ACCUMULATED_GRID_IMPORT, 4)
            if import_regs and len(import_regs) >= 4:
                energy["total_grid_import_kwh"] = round(self._to_unsigned64(import_regs) / self.GAIN_ENERGY, 2)

            # Total grid export (U64, 4 registers, gain 100)
            export_regs = await self._read_input_registers(self.REG_ACCUMULATED_GRID_EXPORT, 4)
            if export_regs and len(export_regs) >= 4:
                energy["total_grid_export_kwh"] = round(self._to_unsigned64(export_regs) / self.GAIN_ENERGY, 2)

            _LOGGER.debug(f"Sigenergy energy summary: {energy}")
            return energy

        except Exception as e:
            _LOGGER.error(f"Error reading energy summary: {e}")
            return {"error": str(e)}

    async def get_status(self) -> InverterState:
        """Get current status of the Sigenergy system.

        Tries plant-level registers first, falls back to inverter-level if those fail.

        Returns:
            InverterState with current status and power readings
        """
        try:
            if not await self.connect():
                return InverterState(
                    status=InverterStatus.OFFLINE,
                    is_curtailed=False,
                    error_message="Failed to connect to Sigenergy",
                )

            attrs = {}

            # Determine which register set to use
            if self._use_inverter_registers is None:
                # First time - try plant registers, then inverter if plant fails
                plant_attrs = await self._read_plant_registers()
                if plant_attrs.get("_success_count", 0) >= 2:
                    attrs = plant_attrs
                    self._use_inverter_registers = False
                    _LOGGER.info("Sigenergy: Using plant-level registers")
                else:
                    # Try inverter-level registers with inverter slave ID
                    inv_attrs = await self._read_inverter_registers()
                    if inv_attrs.get("_success_count", 0) >= 2:
                        attrs = inv_attrs
                        self._use_inverter_registers = True
                        _LOGGER.info(f"Sigenergy: Using inverter-level registers with slave ID {self._inverter_slave_id} (plant registers unavailable)")
                    else:
                        # Neither worked - return what we have
                        attrs = plant_attrs if plant_attrs.get("_success_count", 0) > inv_attrs.get("_success_count", 0) else inv_attrs
                        _LOGGER.warning(f"Sigenergy: Limited register access (plant={plant_attrs.get('_success_count', 0)}, inverter={inv_attrs.get('_success_count', 0)})")
            elif self._use_inverter_registers:
                attrs = await self._read_inverter_registers()
            else:
                attrs = await self._read_plant_registers()

            # Clean up internal tracking fields
            attrs.pop("_success_count", None)
            register_level = attrs.pop("_register_level", "unknown")

            # Read export-related limits for curtailment status and visibility
            # (only available at plant level)
            export_limit = None
            pcs_export_limit = None
            is_curtailed = False
            if not self._use_inverter_registers:
                export_limit_regs = await self._read_holding_registers(self.REG_GRID_EXPORT_LIMIT, 2)
                if export_limit_regs and len(export_limit_regs) >= 2:
                    export_limit = self._to_unsigned32(export_limit_regs[0], export_limit_regs[1])
                    is_curtailed = export_limit < 100  # Less than 0.1 kW threshold
                    if export_limit == self.EXPORT_LIMIT_INVALID:
                        attrs["export_limit_kw"] = "invalid"
                    elif export_limit < self.EXPORT_LIMIT_UNLIMITED:
                        attrs["export_limit_kw"] = round(export_limit / self.GAIN_POWER, 2)
                    else:
                        attrs["export_limit_kw"] = "unlimited"

                pcs_export_limit_regs = await self._read_holding_registers(self.REG_PCS_EXPORT_LIMIT, 2)
                if pcs_export_limit_regs and len(pcs_export_limit_regs) >= 2:
                    pcs_export_limit = self._to_unsigned32(
                        pcs_export_limit_regs[0], pcs_export_limit_regs[1]
                    )
                    if pcs_export_limit == self.EXPORT_LIMIT_INVALID:
                        attrs["pcs_export_limit_kw"] = "invalid"
                    elif pcs_export_limit < self.EXPORT_LIMIT_UNLIMITED:
                        attrs["pcs_export_limit_kw"] = round(pcs_export_limit / self.GAIN_POWER, 2)
                    else:
                        attrs["pcs_export_limit_kw"] = "unlimited"

            # If we couldn't read ANY meaningful registers, the inverter is likely sleeping/offline
            if not attrs or len(attrs) == 0:
                _LOGGER.debug("Sigenergy: No register data - inverter likely sleeping")
                return InverterState(
                    status=InverterStatus.OFFLINE,
                    is_curtailed=False,
                    error_message="No register data (inverter sleeping)",
                    attributes={"host": self.host, "model": self.model or "Sigenergy"},
                )

            # Get PV power for status determination
            pv_power_w = attrs.get("pv_power_w")

            # Determine overall status
            if is_curtailed:
                status = InverterStatus.CURTAILED
                attrs["curtailment_mode"] = "self_consumption_zero_export"
            elif pv_power_w is not None and pv_power_w > 0:
                status = InverterStatus.ONLINE
            else:
                status = InverterStatus.ONLINE  # Connected but no PV production

            # Add model info
            attrs["model"] = self.model or "Sigenergy"
            attrs["host"] = self.host
            attrs["register_level"] = register_level
            attrs["read_only_mode"] = self._read_only

            # In self-consumption zero-export mode, PV is not directly power-limited.
            self._last_state = InverterState(
                status=status,
                is_curtailed=is_curtailed,
                power_output_w=pv_power_w,
                power_limit_percent=100,  # Export is limited at grid point, not PV output.
                attributes=attrs,
            )

            return self._last_state

        except Exception as e:
            _LOGGER.error(f"Error getting Sigenergy status: {e}")
            return InverterState(
                status=InverterStatus.ERROR,
                is_curtailed=False,
                error_message=str(e),
            )

    async def set_pv_power_limit(self, limit_kw: float) -> bool:
        """Set a specific PV power limit.

        Args:
            limit_kw: Power limit in kW (0 = curtail, very high = no limit)

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            # Convert kW to scaled value (multiply by gain)
            scaled_value = int(limit_kw * self.GAIN_POWER)
            if scaled_value < 0:
                scaled_value = 0
            if scaled_value > 0xFFFFFFFE:
                scaled_value = 0xFFFFFFFE  # Max valid value

            _LOGGER.info(f"Setting Sigenergy PV limit to {limit_kw} kW")
            values = self._from_unsigned32(scaled_value)
            return await self._write_holding_registers(self.REG_PV_MAX_POWER_LIMIT, values)

        except Exception as e:
            _LOGGER.error(f"Error setting PV power limit: {e}")
            return False

    async def set_export_limit(self, limit_kw: float) -> bool:
        """Set a specific grid export limit.

        Args:
            limit_kw: Export limit in kW (0 = no export)

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            requested_kw = float(limit_kw)
            if requested_kw < 0:
                requested_kw = 0

            safety_cap_kw = await self._get_effective_export_safety_cap_kw()
            if safety_cap_kw is not None and requested_kw > safety_cap_kw:
                _LOGGER.warning(
                    "Requested export limit %.2f kW exceeds safety cap %.2f kW; clamping",
                    requested_kw,
                    safety_cap_kw,
                )
                requested_kw = safety_cap_kw

            scaled_value = int(requested_kw * self.GAIN_POWER)
            if scaled_value < 0:
                scaled_value = 0
            if scaled_value > 0xFFFFFFFE:
                scaled_value = 0xFFFFFFFE

            _LOGGER.info(f"Setting Sigenergy export limit to {requested_kw} kW")
            values = self._from_unsigned32(scaled_value)
            return await self._write_holding_registers(self.REG_GRID_EXPORT_LIMIT, values)

        except Exception as e:
            _LOGGER.error(f"Error setting export limit: {e}")
            return False

    async def restore_export_limit(self) -> bool:
        """Restore the export limit to the effective safety cap.

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            safety_cap_kw = await self._get_effective_export_safety_cap_kw()
            if safety_cap_kw is None:
                _LOGGER.error("Cannot restore export limit: no configured/finite safety cap available")
                return False

            _LOGGER.info("Restoring Sigenergy export limit to safety cap %.2f kW", safety_cap_kw)
            success = await self.set_export_limit(safety_cap_kw)

            if success:
                _LOGGER.info("Successfully restored Sigenergy export limit to safety cap")
                # Clear stored original limit
                self._original_pv_limit = None
            else:
                _LOGGER.error("Failed to restore Sigenergy export limit")

            return success

        except Exception as e:
            _LOGGER.error(f"Error restoring export limit: {e}")
            return False

    async def set_charge_rate_limit(self, limit_kw: float) -> bool:
        """Set the maximum battery charge rate.

        Args:
            limit_kw: Charge rate limit in kW (0 to disable charging)

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            scaled_value = int(limit_kw * self.GAIN_POWER)
            if scaled_value < 0:
                scaled_value = 0
            if scaled_value > 0xFFFFFFFE:
                scaled_value = 0xFFFFFFFE

            _LOGGER.info(f"Setting Sigenergy charge rate limit to {limit_kw} kW")
            values = self._from_unsigned32(scaled_value)
            return await self._write_holding_registers(self.REG_ESS_MAX_CHARGE_LIMIT, values)

        except Exception as e:
            _LOGGER.error(f"Error setting charge rate limit: {e}")
            return False

    async def set_discharge_rate_limit(self, limit_kw: float) -> bool:
        """Set the maximum battery discharge rate.

        Args:
            limit_kw: Discharge rate limit in kW (0 to disable discharging)

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            scaled_value = int(limit_kw * self.GAIN_POWER)
            if scaled_value < 0:
                scaled_value = 0
            if scaled_value > 0xFFFFFFFE:
                scaled_value = 0xFFFFFFFE

            _LOGGER.info(
                "Setting Sigenergy discharge rate limit to %.2f kW "
                "(note: export control should use grid export limit register)",
                limit_kw,
            )
            values = self._from_unsigned32(scaled_value)
            return await self._write_holding_registers(self.REG_ESS_MAX_DISCHARGE_LIMIT, values)

        except Exception as e:
            _LOGGER.error(f"Error setting discharge rate limit: {e}")
            return False

    async def force_charge(self, power_kw: float = 10.0) -> bool:
        """Force battery to charge from grid.

        Enables Remote EMS mode, sets control mode to charge, and sets charge rate.

        Args:
            power_kw: Charge power in kW (default: 10.0)

        Returns:
            True if all commands successful
        """
        try:
            if not await self.connect():
                return False

            # 1. Enable Remote EMS
            ems_result = await self._set_remote_ems_enabled(True)
            if not ems_result:
                _LOGGER.error("Failed to enable Remote EMS for force charge")
                return False
            _LOGGER.info("Remote EMS enabled for force charge")

            # 2. Set control mode to charge (mode 3)
            mode_result = await self._set_remote_ems_control_mode(
                self.REMOTE_EMS_MODE_COMMAND_CHARGING_GRID_FIRST
            )
            if not mode_result:
                _LOGGER.error("Failed to set Remote EMS control mode to charge")
                return False
            _LOGGER.info("Remote EMS control mode set to CHARGE")

            # 3. Set charge rate limit
            rate_result = await self.set_charge_rate_limit(power_kw)
            if not rate_result:
                _LOGGER.error(f"Failed to set charge rate limit to {power_kw} kW")
                return False

            _LOGGER.info(f"Sigenergy FORCE CHARGE active at {power_kw} kW")
            return True

        except Exception as e:
            _LOGGER.error(f"Error in Sigenergy force charge: {e}")
            return False

    async def force_discharge(self, power_kw: float = 10.0) -> bool:
        """Force battery to discharge to grid/load.

        Enables Remote EMS mode, sets control mode to discharge, and sets
        grid export limit.

        Note: For export control we intentionally use REG_GRID_EXPORT_LIMIT
        instead of REG_ESS_MAX_DISCHARGE_LIMIT. The ESS discharge register
        caps total battery output (home load + grid export), while grid export
        limit constrains only export to the grid.

        Args:
            power_kw: Discharge power in kW (default: 10.0)

        Returns:
            True if all commands successful
        """
        try:
            if not await self.connect():
                return False

            # 1. Enable Remote EMS
            ems_result = await self._set_remote_ems_enabled(True)
            if not ems_result:
                _LOGGER.error("Failed to enable Remote EMS for force discharge")
                return False
            _LOGGER.info("Remote EMS enabled for force discharge")

            # 2. Set control mode to discharge (mode 5: output from PV first)
            mode_result = await self._set_remote_ems_control_mode(
                self.REMOTE_EMS_MODE_COMMAND_DISCHARGING_PV_FIRST
            )
            if not mode_result:
                _LOGGER.error("Failed to set Remote EMS control mode to discharge")
                return False
            _LOGGER.info("Remote EMS control mode set to DISCHARGE")

            # 3. Set export limit (do not cap total battery discharge)
            export_result = await self.set_export_limit(power_kw)
            if not export_result:
                _LOGGER.error(f"Failed to set grid export limit to {power_kw} kW")
                return False

            _LOGGER.info(f"Sigenergy FORCE DISCHARGE active with export limit {power_kw} kW")
            return True

        except Exception as e:
            _LOGGER.error(f"Error in Sigenergy force discharge: {e}")
            return False

    async def restore_normal(self) -> bool:
        """Restore normal self-consumption behavior in Remote EMS.

        Keeps Remote EMS enabled and switches control mode to
        Maximum self-consumption (mode 2).

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            # Keep Remote EMS enabled and switch to maximum self-consumption.
            ems_result = await self._set_remote_ems_enabled(True)
            if not ems_result:
                _LOGGER.error("Failed to enable Remote EMS for normal mode restore")
                return False

            mode_result = await self._set_remote_ems_control_mode(
                self.REMOTE_EMS_MODE_MAX_SELF_CONSUMPTION
            )
            if not mode_result:
                _LOGGER.error("Failed to set Remote EMS control mode to max self-consumption")
                return False

            # Reset ESS max charge/discharge limits back to rated defaults.
            # Export behavior is controlled separately via REG_GRID_EXPORT_LIMIT.
            defaults_restored = await self._restore_ess_max_limits_to_rated()
            if not defaults_restored:
                _LOGGER.warning(
                    "Could not fully restore ESS max charge/discharge limits to rated values"
                )

            _LOGGER.info("Sigenergy normal operation restored in Remote EMS (max self-consumption)")
            return True

        except Exception as e:
            _LOGGER.error(f"Error restoring Sigenergy normal operation: {e}")
            return False

    async def _set_remote_ems_enabled(self, enabled: bool) -> bool:
        """Enable or disable Remote EMS mode."""
        value = 1 if enabled else 0
        return await self._write_holding_registers(self.REG_REMOTE_EMS_ENABLE, [value])

    async def _set_remote_ems_control_mode(self, mode: int) -> bool:
        """Set Remote EMS control mode."""
        return await self._write_holding_registers(self.REG_REMOTE_EMS_CONTROL_MODE, [mode])

    async def _restore_ess_max_limits_to_rated(self) -> bool:
        """Restore ESS max charge/discharge limits to ESS rated powers.

        Returns True when both writes succeed (or when in read-only mode they are
        intentionally blocked and this returns False).
        """
        rated_charge_kw, rated_discharge_kw = await self._get_ess_rated_charge_discharge_power_kw()

        if rated_charge_kw is None and rated_discharge_kw is None:
            _LOGGER.warning("ESS rated charge/discharge powers unavailable; cannot restore ESS max limits")
            return False

        ok_charge = True
        ok_discharge = True

        if rated_charge_kw is not None:
            _LOGGER.info("Restoring ESS max charge limit to rated value %.2f kW", rated_charge_kw)
            ok_charge = await self.set_charge_rate_limit(rated_charge_kw)

        if rated_discharge_kw is not None:
            _LOGGER.info("Restoring ESS max discharge limit to rated value %.2f kW", rated_discharge_kw)
            ok_discharge = await self.set_discharge_rate_limit(rated_discharge_kw)

        return ok_charge and ok_discharge

    async def get_backup_reserve(self) -> Optional[int]:
        """Get the current backup reserve (backup SOC) percentage.

        Returns:
            Backup reserve percentage (0-100) or None on error
        """
        try:
            if not await self.connect():
                return None

            regs = await self._read_holding_registers(self.REG_ESS_BACKUP_SOC, 1)
            if regs and len(regs) >= 1:
                # U16 with gain 10
                reserve_pct = regs[0] / self.GAIN_SOC
                _LOGGER.debug(f"Sigenergy backup reserve: {reserve_pct}%")
                return int(reserve_pct)
            return None

        except Exception as e:
            _LOGGER.error(f"Error getting backup reserve: {e}")
            return None

    async def set_backup_reserve(self, percent: int) -> bool:
        """Set the backup reserve (backup SOC) percentage.

        This is the minimum battery level that will be preserved.
        The battery won't discharge below this level except in backup/outage mode.

        Args:
            percent: Backup reserve percentage (0-100)

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            if percent < 0:
                percent = 0
            if percent > 100:
                percent = 100

            # U16 with gain 10
            scaled_value = int(percent * self.GAIN_SOC)

            _LOGGER.info(f"Setting Sigenergy backup reserve to {percent}%")
            success = await self._write_holding_registers(self.REG_ESS_BACKUP_SOC, [scaled_value])

            if success:
                _LOGGER.info(f"✅ Successfully set Sigenergy backup reserve to {percent}%")
            else:
                _LOGGER.error(f"Failed to set Sigenergy backup reserve")

            return success

        except Exception as e:
            _LOGGER.error(f"Error setting backup reserve: {e}")
            return False

    async def get_discharge_cutoff_soc(self) -> Optional[int]:
        """Get the discharge cut-off SOC percentage.

        Returns:
            Discharge cut-off SOC percentage (0-100) or None on error
        """
        try:
            if not await self.connect():
                return None

            regs = await self._read_holding_registers(self.REG_ESS_DISCHARGE_CUTOFF_SOC, 1)
            if regs and len(regs) >= 1:
                cutoff_pct = regs[0] / self.GAIN_SOC
                _LOGGER.debug(f"Sigenergy discharge cut-off SOC: {cutoff_pct}%")
                return int(cutoff_pct)
            return None

        except Exception as e:
            _LOGGER.error(f"Error getting discharge cut-off SOC: {e}")
            return None

    async def set_discharge_cutoff_soc(self, percent: int) -> bool:
        """Set the discharge cut-off SOC percentage.

        The battery will stop discharging when it reaches this level.

        Args:
            percent: Discharge cut-off SOC percentage (0-100)

        Returns:
            True if successful
        """
        try:
            if not await self.connect():
                return False

            if percent < 0:
                percent = 0
            if percent > 100:
                percent = 100

            scaled_value = int(percent * self.GAIN_SOC)

            _LOGGER.info(f"Setting Sigenergy discharge cut-off SOC to {percent}%")
            success = await self._write_holding_registers(self.REG_ESS_DISCHARGE_CUTOFF_SOC, [scaled_value])

            if success:
                _LOGGER.info(f"✅ Successfully set Sigenergy discharge cut-off SOC to {percent}%")
            else:
                _LOGGER.error(f"Failed to set Sigenergy discharge cut-off SOC")

            return success

        except Exception as e:
            _LOGGER.error(f"Error setting discharge cut-off SOC: {e}")
            return False

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
