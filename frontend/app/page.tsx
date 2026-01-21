'use client'

import { useState } from 'react'
import DownloadForm from '@/components/DownloadForm'
import DeleteForm from '@/components/DeleteForm'
import EditDataForm from '@/components/EditDataForm'
import KlineViewer from '@/components/KlineViewer'
import SymbolListWithChart from '@/components/SymbolListWithChart'
import DataIntegrityChecker from '@/components/DataIntegrityChecker'
import IPInfo from '@/components/IPInfo'
import DataMigration from '@/components/DataMigration'
import { TopGainersProvider } from '@/contexts/TopGainersContext'

export default function DataDashboard() {
  const [activeTab, setActiveTab] = useState<'download' | 'delete' | 'edit' | 'kline' | 'list-chart' | 'integrity' | 'ip-info' | 'migration'>('download')

  return (
    <TopGainersProvider>
      <main className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white">
        <div className="container mx-auto px-4 py-8">
          <header className="mb-8">
            <div>
              <h1 className="text-4xl font-bold mb-2 bg-gradient-to-r from-blue-400 to-purple-600 bg-clip-text text-transparent">
                数据管理 Dashboard
              </h1>
              <p className="text-gray-400">管理和维护币安U本位合约K线数据</p>
            </div>
          </header>

          {/* 标签页导航 */}
          <div className="flex space-x-4 mb-6 border-b border-gray-700 overflow-x-auto">
            <button
              onClick={() => setActiveTab('download')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'download'
                  ? 'text-blue-400 border-b-2 border-blue-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              下载数据
            </button>
            <button
              onClick={() => setActiveTab('delete')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'delete'
                  ? 'text-red-400 border-b-2 border-red-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              删除数据
            </button>
            <button
              onClick={() => setActiveTab('edit')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'edit'
                  ? 'text-yellow-400 border-b-2 border-yellow-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              修改数据
            </button>
            <button
              onClick={() => setActiveTab('kline')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'kline'
                  ? 'text-blue-400 border-b-2 border-blue-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              查看K线
            </button>
            <button
              onClick={() => setActiveTab('list-chart')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'list-chart'
                  ? 'text-blue-400 border-b-2 border-blue-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              列表与图表
            </button>
            <button
              onClick={() => setActiveTab('integrity')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'integrity'
                  ? 'text-green-400 border-b-2 border-green-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              完整性检查
            </button>
            <button
              onClick={() => setActiveTab('ip-info')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'ip-info'
                  ? 'text-purple-400 border-b-2 border-purple-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              IP地址信息
            </button>
            <button
              onClick={() => setActiveTab('migration')}
              className={`px-6 py-3 font-medium transition-colors whitespace-nowrap ${
                activeTab === 'migration'
                  ? 'text-orange-400 border-b-2 border-orange-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              数据迁移
            </button>
          </div>

          {/* 内容区域 */}
          <div 
            className={`bg-gray-800/50 backdrop-blur-sm rounded-lg shadow-xl ${
              activeTab === 'list-chart' ? 'p-4' : 'p-6'
            }`}
            style={activeTab === 'list-chart' ? { height: 'calc(100vh - 250px)', minHeight: '600px' } : {}}
          >
            {activeTab === 'download' && <DownloadForm />}
            {activeTab === 'delete' && <DeleteForm />}
            {activeTab === 'edit' && <EditDataForm />}
            {activeTab === 'kline' && <KlineViewer />}
            {activeTab === 'list-chart' && <SymbolListWithChart />}
            {activeTab === 'integrity' && <DataIntegrityChecker />}
            {activeTab === 'ip-info' && <IPInfo />}
            {activeTab === 'migration' && <DataMigration />}
          </div>
        </div>
      </main>
    </TopGainersProvider>
  )
}
