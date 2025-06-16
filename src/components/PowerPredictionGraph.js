'use client';

import { useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { motion } from 'framer-motion';
import { RefreshCw, Clock, Database, AlertCircle } from 'lucide-react';

export default function PowerPredictionGraph() {
  const [predictions, setPredictions] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [fromCache, setFromCache] = useState(false);
  const [debugInfo, setDebugInfo] = useState('');

  const fetchPredictions = async (forceRefresh = false) => {
    const debugLog = [];

    try {
      debugLog.push(`Starting fetch - forceRefresh: ${forceRefresh}`);

      if (forceRefresh) {
        setRefreshing(true);
        debugLog.push('Setting refreshing state to true');
      } else {
        setLoading(true);
        debugLog.push('Setting loading state to true');
      }

      const url = forceRefresh ? '/api/predictions?refresh=true' : '/api/predictions';
      debugLog.push(`Fetching from URL: ${url}`);

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      debugLog.push(`Response status: ${response.status}`);
      debugLog.push(`Response ok: ${response.ok}`);

      if (!response.ok) {
        const errorText = await response.text();
        debugLog.push(`Error response text: ${errorText}`);
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const data = await response.json();
      debugLog.push(`Response data received - predictions: ${data.predictions?.length}, fromCache: ${data.fromCache}`);

      if (!data.predictions || !Array.isArray(data.predictions)) {
        debugLog.push('Invalid data structure received');
        throw new Error('Invalid response data structure');
      }

      setPredictions(data.predictions);
      setLastUpdated(new Date(data.lastUpdated));
      setFromCache(data.fromCache);
      setError(null);
      debugLog.push('State updated successfully');

    } catch (err) {
      debugLog.push(`Error caught: ${err.message}`);
      console.error('Fetch error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
      setDebugInfo(debugLog.join('\n'));
      debugLog.push('Loading states reset');
    }
  };

  useEffect(() => {
    console.log('Component mounted, fetching initial predictions');
    fetchPredictions();
  }, []);

  const handleRefresh = () => {
    console.log('Refresh button clicked');
    fetchPredictions(true);
  };

  const formatLastUpdated = (date) => {
    if (!date) return 'Unknown';

    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins} min ago`;
    if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;
    return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;
  };

  if (loading) {
    return (
        <Card className="w-full p-4 bg-gray-800/50 backdrop-blur-sm border-gray-700">
          <div className="flex items-center justify-center h-64">
            <motion.div
                initial={{ scale: 0.5, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                transition={{ duration: 0.5 }}
                className="relative"
            >
              <div className="w-16 h-16 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="w-12 h-12 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin" style={{ animationDirection: 'reverse' }}></div>
              </div>
            </motion.div>
          </div>
        </Card>
    );
  }

  if (error) {
    return (
        <Card className="w-full p-4 bg-gray-800/50 backdrop-blur-sm border-gray-700">
          <div className="flex flex-col items-center justify-center h-64 space-y-4">
            <div className="text-red-500 text-center">
              <AlertCircle className="w-12 h-12 mx-auto mb-2" />
              <p className="font-semibold">Error loading predictions</p>
              <p className="text-sm text-gray-400 mt-1">{error}</p>
            </div>
            <button
                onClick={handleRefresh}
                disabled={refreshing}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-800 text-white rounded-lg transition-colors duration-200"
            >
              {refreshing ? 'Retrying...' : 'Try Again'}
            </button>

            {/* Debug information */}
            {process.env.NODE_ENV === 'development' && debugInfo && (
                <details className="mt-4 w-full">
                  <summary className="text-xs text-gray-500 cursor-pointer">Debug Info</summary>
                  <pre className="text-xs text-gray-400 mt-2 bg-gray-900 p-2 rounded overflow-auto max-h-32">
                {debugInfo}
              </pre>
                </details>
            )}
          </div>
        </Card>
    );
  }

  if (!predictions?.length) {
    return (
        <Card className="w-full p-4 bg-gray-800/50 backdrop-blur-sm border-gray-700">
          <div className="flex flex-col items-center justify-center h-64 space-y-4">
            <div className="text-gray-400 text-center">
              <Database className="w-12 h-12 mx-auto mb-2" />
              <p>No prediction data available</p>
              <p className="text-sm">Click generate to create predictions</p>
            </div>
            <button
                onClick={handleRefresh}
                disabled={refreshing}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-800 text-white rounded-lg transition-colors duration-200"
            >
              {refreshing ? 'Generating...' : 'Generate Predictions'}
            </button>
          </div>
        </Card>
    );
  }

  const chartData = predictions.map(pred => ({
    date: new Date(pred.date).toLocaleDateString('en-US', {
      weekday: 'short',
      month: 'short',
      day: 'numeric'
    }),
    predicted: pred.predictedPower,
    confidence: pred.confidence * 100
  }));

  return (
      <Card className="w-full p-4 bg-gray-800/50 backdrop-blur-sm border-gray-700 hover:bg-gray-800/70 transition-all duration-300">
        {/* Header with title and refresh button */}
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-white">7-Day Power Generation Forecast</h2>
          <div className="flex items-center space-x-3">
            {/* Cache indicator */}
            <div className="flex items-center space-x-1 text-xs text-gray-400">
              {fromCache ? (
                  <>
                    <Database className="w-3 h-3" />
                    <span>Cached</span>
                  </>
              ) : (
                  <>
                    <RefreshCw className="w-3 h-3" />
                    <span>Fresh</span>
                  </>
              )}
            </div>

            {/* Last updated */}
            <div className="flex items-center space-x-1 text-xs text-gray-400">
              <Clock className="w-3 h-3" />
              <span>{formatLastUpdated(lastUpdated)}</span>
            </div>

            {/* Refresh button */}
            <button
                onClick={handleRefresh}
                disabled={refreshing}
                className="flex items-center space-x-1 px-3 py-1 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-800 text-white text-sm rounded-lg transition-colors duration-200"
            >
              <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
              <span>{refreshing ? 'Updating...' : 'Refresh'}</span>
            </button>
          </div>
        </div>

        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                  dataKey="date"
                  stroke="#9CA3AF"
                  tick={{ fill: '#9CA3AF', fontSize: 12 }}
              />
              <YAxis
                  stroke="#9CA3AF"
                  tick={{ fill: '#9CA3AF', fontSize: 12 }}
                  label={{
                    value: 'Power (W)',
                    angle: -90,
                    position: 'insideLeft',
                    fill: '#9CA3AF'
                  }}
              />
              <Tooltip
                  contentStyle={{
                    backgroundColor: '#1F2937',
                    border: '1px solid #374151',
                    borderRadius: '0.375rem',
                    color: '#F3F4F6'
                  }}
                  formatter={(value, name) => [
                    `${value}${name === 'Predicted Power' ? 'W' : '%'}`,
                    name === 'Predicted Power' ? 'Predicted Power' : 'Confidence'
                  ]}
              />
              <Legend />
              <Line
                  type="monotone"
                  dataKey="predicted"
                  name="Predicted Power"
                  stroke="#60A5FA"
                  strokeWidth={2}
                  dot={{ fill: '#60A5FA', strokeWidth: 2 }}
                  activeDot={{ r: 6 }}
              />
              <Line
                  type="monotone"
                  dataKey="confidence"
                  name="Confidence %"
                  stroke="#34D399"
                  strokeWidth={2}
                  dot={{ fill: '#34D399', strokeWidth: 2 }}
                  activeDot={{ r: 6 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="mt-4 p-3 bg-gray-700/50 rounded-lg">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div className="text-center">
              <div className="text-gray-400">Avg. Predicted</div>
              <div className="text-white font-semibold">
                {Math.round(predictions.reduce((sum, p) => sum + p.predictedPower, 0) / predictions.length)}W
              </div>
            </div>
            <div className="text-center">
              <div className="text-gray-400">Best Day</div>
              <div className="text-green-400 font-semibold">
                {Math.max(...predictions.map(p => p.predictedPower))}W
              </div>
            </div>
            <div className="text-center">
              <div className="text-gray-400">Worst Day</div>
              <div className="text-red-400 font-semibold">
                {Math.min(...predictions.map(p => p.predictedPower))}W
              </div>
            </div>
            <div className="text-center">
              <div className="text-gray-400">Avg. Confidence</div>
              <div className="text-blue-400 font-semibold">
                {Math.round(predictions.reduce((sum, p) => sum + p.confidence, 0) / predictions.length * 100)}%
              </div>
            </div>
          </div>
        </div>

        {process.env.NODE_ENV === 'development' && (
            <div className="mt-2 text-xs text-gray-500">
              Predictions: {predictions.length} | From Cache: {fromCache ? 'Yes' : 'No'}
            </div>
        )}
      </Card>
  );
}