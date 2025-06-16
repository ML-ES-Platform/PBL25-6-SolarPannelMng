import { Card } from '@/components/ui/card'
import { LineChart, Line, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

export function SystemStatus({ data, history }) {
  const inverterTemp = data.inverter_heat_sink_temperature || 0
  const isLoadOn = data.is_load_on === 1
  const isSystemOn = data.is_switched_on === 1
  const isSccCharging = data.is_scc_charging_on === 1
  const isAcCharging = data.is_ac_charging_on === 1

  const getTemperatureColor = (temp) => {
    if (temp >= 60) return 'text-red-500'
    if (temp >= 45) return 'text-yellow-500'
    return 'text-emerald-500'
  }

  const formatHistoryData = (history) => {
    if (!history) return []
    return history.map(item => {
      const date = new Date(item.timestamp.$date || item.timestamp)
      return {
        timestamp: date.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', hour12: true }),
        temperature: item.data.inverter_heat_sink_temperature
      }
    })
  }

  return (
    <Card className="h-full p-4 sm:p-6 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300 transform hover:scale-[1.02] hover:shadow-2xl hover:shadow-purple-500/10">
      <h2 className="text-xl sm:text-2xl font-semibold mb-4 text-white">System Status</h2>
      <div className="flex flex-col h-[calc(100%-3rem)]">
        <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm mb-4">
          <p className="text-sm text-gray-400">Inverter Temperature</p>
          <div className="flex items-baseline gap-1">
            <p className={`text-lg font-semibold ${getTemperatureColor(inverterTemp)}`}>{inverterTemp}</p>
            <p className="text-sm font-medium text-red-400">°C</p>
          </div>
          <div className="h-16 mt-2">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={formatHistoryData(history)} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                <defs>
                  <linearGradient id="tempGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#EF4444" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#EF4444" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.1} />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#1F2937',
                    border: 'none',
                    borderRadius: '0.5rem',
                    color: '#F3F4F6',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
                    fontSize: '11px'
                  }}
                  labelStyle={{ color: '#F3F4F6', fontSize: '11px' }}
                  formatter={(value) => [`${value}°C`, 'Temperature']}
                  labelFormatter={(label, payload) => {
                    if (!payload || !payload[0] || !payload[0].payload) return label
                    return payload[0].payload.timestamp
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="temperature"
                  stroke="#EF4444"
                  strokeWidth={1}
                  dot={false}
                  isAnimationActive={false}
                  activeDot={{ r: 4, fill: '#EF4444' }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3 flex-grow">
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <div className="flex items-center gap-2">
              <div className={`w-3 h-3 rounded-full ${isLoadOn ? 'bg-emerald-500' : 'bg-gray-400'}`} />
              <span className="text-sm text-gray-300">Load Status</span>
            </div>
            <p className="text-lg font-semibold text-white mt-2">{isLoadOn ? 'On' : 'Off'}</p>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <div className="flex items-center gap-2">
              <div className={`w-3 h-3 rounded-full ${isSystemOn ? 'bg-emerald-500' : 'bg-gray-400'}`} />
              <span className="text-sm text-gray-300">System Status</span>
            </div>
            <p className="text-lg font-semibold text-white mt-2">{isSystemOn ? 'On' : 'Off'}</p>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <div className="flex items-center gap-2">
              <div className={`w-3 h-3 rounded-full ${isSccCharging ? 'bg-emerald-500' : 'bg-gray-400'}`} />
              <span className="text-sm text-gray-300">SCC Charging</span>
            </div>
            <p className="text-lg font-semibold text-white mt-2">{isSccCharging ? 'Active' : 'Inactive'}</p>
          </div>
          <div className="bg-gray-700/50 p-3 rounded-lg backdrop-blur-sm">
            <div className="flex items-center gap-2">
              <div className={`w-3 h-3 rounded-full ${isAcCharging ? 'bg-emerald-500' : 'bg-gray-400'}`} />
              <span className="text-sm text-gray-300">AC Charging</span>
            </div>
            <p className="text-lg font-semibold text-white mt-2">{isAcCharging ? 'Active' : 'Inactive'}</p>
          </div>
        </div>
      </div>
    </Card>
  )
} 