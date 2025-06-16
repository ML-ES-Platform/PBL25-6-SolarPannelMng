import { LastRefresh } from './LastRefresh'
import Image from 'next/image'

export function Navbar({ lastUpdateTime }) {
  const handleExport = async (timeRange) => {
    try {
      console.log('Exporting report for:', timeRange)
      const response = await fetch(`/api/export-report?timeRange=${timeRange}`)
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }
      
      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `solar-report-${timeRange}-${new Date().toISOString().split('T')[0]}.pdf`
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)
    } catch (error) {
      console.error('Error exporting report:', error)
      alert(`Failed to export report: ${error.message}`)
    }
  }

  return (
    <nav className="bg-gray-800/50 backdrop-blur-sm border-b border-gray-700">
      <div className="max-w-[1400px] mx-auto px-4 sm:px-6 py-4">
        <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <Image
              src="/UTM_Logo.png"
              alt="UTM Logo"
              height={40}
              width={200}
              quality={100}
              priority
              className="w-auto h-10"
            />
            <h1 className="text-3xl sm:text-4xl font-bold bg-gradient-to-r from-blue-400 via-emerald-400 to-blue-400 bg-clip-text text-transparent animate-gradient">
                Solar Panels
            </h1>
          </div>
          <div className="flex items-center gap-4">
            {lastUpdateTime && (
              <div className="flex items-center gap-2 text-gray-400">
                <LastRefresh timestamp={lastUpdateTime} />
              </div>
            )}
            <div className="flex flex-col sm:flex-row gap-2">
              <button
                onClick={() => handleExport('today')}
                className="inline-flex items-center px-3 py-1.5 text-sm font-medium rounded-lg bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white transition-all duration-200 hover:shadow-lg hover:shadow-gray-500/10"
              >
                <svg className="w-4 h-4 mr-2 inline-block" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                Today
              </button>
              <button
                onClick={() => handleExport('week')}
                className="inline-flex items-center px-3 py-1.5 text-sm font-medium rounded-lg bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white transition-all duration-200 hover:shadow-lg hover:shadow-gray-500/10"
              >
                <svg className="w-4 h-4 mr-2 inline-block" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                Week
              </button>
              <button
                onClick={() => handleExport('month')}
                className="inline-flex items-center px-3 py-1.5 text-sm font-medium rounded-lg bg-gray-700/50 text-gray-300 hover:bg-gray-600/50 hover:text-white transition-all duration-200 hover:shadow-lg hover:shadow-gray-500/10"
              >
                <svg className="w-4 h-4 mr-2 inline-block" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                Month
              </button>
            </div>
          </div>
        </div>
      </div>
    </nav>
  )
} 