import { Card } from '@/components/ui/card'

export function PowerStats({ data }) {
  const acInputVoltage = data.ac_input_voltage || 0
  const acInputFrequency = data.ac_input_frequency || 0
  const acOutputVoltage = data.ac_output_voltage || 0
  const acOutputFrequency = data.ac_output_frequency || 0
  const acOutputApparentPower = data.ac_output_apparent_power || 0
  const acOutputActivePower = data.ac_output_active_power || 0
  const acOutputLoad = data.ac_output_load || 0
  const busVoltage = data.bus_voltage || 0
  const pvInputVoltage = data.pv_input_voltage || 0
  const pvInputCurrent = data.pv_input_current_for_battery || 0
  const pvInputPower = data.pv_input_power || 0

  return (
    <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-blue-500/10">
      <h2 className="text-xl sm:text-2xl font-semibold mb-4 text-white">Power Statistics</h2>
      <div className="flex flex-col h-[calc(100%-3rem)]">
        <div className="grid grid-cols-2 gap-3 flex-grow">
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Input Voltage</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acInputVoltage}</p>
              <p className="text-sm font-medium text-blue-400">V</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Input Frequency</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acInputFrequency}</p>
              <p className="text-sm font-medium text-purple-400">Hz</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Output Voltage</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acOutputVoltage}</p>
              <p className="text-sm font-medium text-blue-400">V</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Output Frequency</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acOutputFrequency}</p>
              <p className="text-sm font-medium text-purple-400">Hz</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Apparent Power</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acOutputApparentPower}</p>
              <p className="text-sm font-medium text-amber-400">VA</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Active Power</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acOutputActivePower}</p>
              <p className="text-sm font-medium text-amber-400">W</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">AC Output Load</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{acOutputLoad}</p>
              <p className="text-sm font-medium text-amber-400">%</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">Bus Voltage</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{busVoltage}</p>
              <p className="text-sm font-medium text-blue-400">V</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">PV Input Voltage</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{pvInputVoltage}</p>
              <p className="text-sm font-medium text-blue-400">V</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <p className="text-sm text-gray-400">PV Input Current</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{pvInputCurrent}</p>
              <p className="text-sm font-medium text-emerald-400">A</p>
            </div>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm col-span-2">
            <p className="text-sm text-gray-400">PV Input Power</p>
            <div className="flex items-baseline gap-1">
              <p className="text-lg font-semibold text-white">{pvInputPower}</p>
              <p className="text-sm font-medium text-amber-400">W</p>
            </div>
          </div>
        </div>
      </div>
    </Card>
  )
} 