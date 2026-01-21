'use client'

import { useState } from 'react'
import { API_URLS } from '../lib/api-config'

interface IntegrityResult {
  total_symbols: number
  checked_symbols: number
  symbols_with_issues: string[]
  summary: {
    duplicates: number
    missing_dates: number
    data_quality_issues: number
    empty_tables: number
  }
  details: {
    [symbol: string]: {
      symbol: string
      table_name: string
      record_count: number
      date_range: {
        start: string
        end: string
        days: number
      } | null
      issues: string[]
      duplicate_count: number
      missing_dates: string[]
      data_quality_issues: string[]
      invalid_price_data?: Array<{
        trade_date: string
        open: number | null
        high: number | null
        low: number | null
        close: number | null
        issues: string[]
      }>
    }
  }
}

export default function DataIntegrityChecker() {
  const [symbol, setSymbol] = useState('')
  const [interval, setInterval] = useState('1d')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [checkDuplicates, setCheckDuplicates] = useState(true)
  const [checkMissingDates, setCheckMissingDates] = useState(true)
  const [checkDataQuality, setCheckDataQuality] = useState(true)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<IntegrityResult | null>(null)
  const [error, setError] = useState('')
  const [expandedSymbols, setExpandedSymbols] = useState<Set<string>>(new Set())
  const [downloading, setDownloading] = useState(false)
  const [downloadStats, setDownloadStats] = useState<any>(null)
  const [reportFormat, setReportFormat] = useState('text')
  const [generatingReport, setGeneratingReport] = useState(false)
  const [rechecking, setRechecking] = useState(false)
  const [recheckResult, setRecheckResult] = useState<any>(null)

  const runCheck = async () => {
    setLoading(true)
    setError('')
    setResult(null)

    try {
      const response = await fetch(`${API_URLS.data}/api/data-integrity`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          symbol: symbol || null,
          interval,
          start_date: startDate || null,
          end_date: endDate || null,
          check_duplicates: checkDuplicates,
          check_missing_dates: checkMissingDates,
          check_data_quality: checkDataQuality,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '检查失败')
      }

      const data = await response.json()
      setResult(data)
    } catch (err: any) {
      setError(err.message || '检查失败')
    } finally {
      setLoading(false)
    }
  }

  const toggleSymbol = (symbol: string) => {
    const newExpanded = new Set(expandedSymbols)
    if (newExpanded.has(symbol)) {
      newExpanded.delete(symbol)
    } else {
      newExpanded.add(symbol)
    }
    setExpandedSymbols(newExpanded)
  }

  const downloadMissingData = async () => {
    if (!result) {
      setError('请先执行数据完整性检查')
      return
    }

    setDownloading(true)
    setError('')
    setDownloadStats(null)

    try {
      const response = await fetch(`${API_URLS.data}/api/download-missing-data`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          symbol: symbol || null,
          interval,
          start_date: startDate || null,
          end_date: endDate || null,
          check_duplicates: checkDuplicates,
          check_missing_dates: checkMissingDates,
          check_data_quality: checkDataQuality,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '下载失败')
      }

      const data = await response.json()
      setDownloadStats(data.download_stats)
      
      // 使用下载后的最新检查结果更新显示
      if (data.check_results_after) {
        setResult(data.check_results_after)
      } else {
        // 如果没有返回最新结果，重新执行检查
        await runCheck()
      }
    } catch (err: any) {
      setError(err.message || '下载失败')
    } finally {
      setDownloading(false)
    }
  }

  const generateReport = async () => {
    if (!result) {
      setError('请先执行数据完整性检查')
      return
    }

    setGeneratingReport(true)
    setError('')

    try {
      const response = await fetch(`${API_URLS.data}/api/generate-integrity-report`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          check_results: result,
          interval,
          start_date: startDate || null,
          end_date: endDate || null,
          check_duplicates: checkDuplicates,
          check_missing_dates: checkMissingDates,
          check_data_quality: checkDataQuality,
          output_format: reportFormat,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '生成报告失败')
      }

      const data = await response.json()
      
      // 创建下载链接
      const blob = new Blob([data.report], { 
        type: reportFormat === 'html' ? 'text/html' : 
              reportFormat === 'json' ? 'application/json' :
              reportFormat === 'markdown' ? 'text/markdown' : 'text/plain'
      })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      const extension = reportFormat === 'html' ? 'html' : 
                       reportFormat === 'json' ? 'json' :
                       reportFormat === 'markdown' ? 'md' : 'txt'
      a.download = `data_integrity_report_${interval}_${new Date().toISOString().split('T')[0]}.${extension}`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err: any) {
      setError(err.message || '生成报告失败')
    } finally {
      setGeneratingReport(false)
    }
  }

  const generateDownloadScript = async () => {
    if (!result) {
      setError('请先执行数据完整性检查')
      return
    }

    try {
      const response = await fetch(`${API_URLS.data}/api/generate-download-script`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          check_results: result,
          interval,
          auto_execute: false,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '生成脚本失败')
      }

      const data = await response.json()
      
      // 创建下载链接
      const blob = new Blob([data.script], { type: 'text/plain' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `download_missing_data_${interval}_${new Date().toISOString().split('T')[0]}.sh`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err: any) {
      setError(err.message || '生成脚本失败')
    }
  }

  const recheckProblematicSymbols = async () => {
    if (!result) {
      setError('请先执行数据完整性检查')
      return
    }

    if (!result.symbols_with_issues || result.symbols_with_issues.length === 0) {
      setError('没有需要复检的交易对')
      return
    }

    setRechecking(true)
    setError('')
    setRecheckResult(null)

    try {
      // 生成报告文件名
      const timestamp = new Date().toISOString().split('T')[0]
      const reportFileName = `recheck_report_${interval}_${timestamp}.txt`
      
      const response = await fetch(`${API_URLS.data}/api/recheck-problematic-symbols`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          check_results: result,
          interval,
          start_date: startDate || null,
          end_date: endDate || null,
          output_file: reportFileName,  // 指定输出文件
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '复检失败')
      }

      const data = await response.json()
      setRecheckResult(data.recheck_results)
      
      // 如果后端返回了报告内容，自动下载
      if (data.report_content && data.report_file) {
        try {
          const blob = new Blob([data.report_content], { type: 'text/plain; charset=utf-8' })
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a')
          a.href = url
          a.download = data.report_file
          document.body.appendChild(a)
          a.click()
          document.body.removeChild(a)
          URL.revokeObjectURL(url)
          console.log(`报告文件已下载: ${data.report_file}`)
        } catch (reportErr) {
          console.error('下载报告文件失败:', reportErr)
          setError(`复检完成，但下载报告失败: ${reportErr}`)
        }
      } else {
        console.warn('后端未返回报告内容', { report_content: !!data.report_content, report_file: data.report_file })
      }
    } catch (err: any) {
      setError(err.message || '复检失败')
    } finally {
      setRechecking(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold mb-2 bg-gradient-to-r from-blue-400 to-purple-600 bg-clip-text text-transparent">
          数据完整性检查
        </h2>
        <p className="text-gray-400">检查K线数据的完整性、重复性和质量</p>
      </div>

      {/* 检查配置 */}
      <div className="bg-gray-700/50 rounded-lg p-6 space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium mb-2 text-gray-300">
              交易对符号（可选）
            </label>
            <input
              type="text"
              value={symbol}
              onChange={(e) => setSymbol(e.target.value.toUpperCase())}
              placeholder="例如: BTCUSDT（留空检查所有）"
              className="w-full px-4 py-2 bg-gray-600 border border-gray-500 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-white"
            />
          </div>

          <div>
            <label className="block text-sm font-medium mb-2 text-gray-300">
              K线间隔 <span className="text-red-400">*</span>
            </label>
            <select
              value={interval}
              onChange={(e) => setInterval(e.target.value)}
              className="w-full px-4 py-2 bg-gray-600 border border-gray-500 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-white"
            >
              <optgroup label="分钟">
                <option value="1m">1分钟</option>
                <option value="3m">3分钟</option>
                <option value="5m">5分钟</option>
                <option value="15m">15分钟</option>
                <option value="30m">30分钟</option>
              </optgroup>
              <optgroup label="小时">
                <option value="1h">1小时</option>
                <option value="2h">2小时</option>
                <option value="4h">4小时</option>
                <option value="6h">6小时</option>
                <option value="8h">8小时</option>
                <option value="12h">12小时</option>
              </optgroup>
              <optgroup label="日/周/月">
                <option value="1d">1日</option>
                <option value="3d">3日</option>
                <option value="1w">1周</option>
                <option value="1M">1月</option>
              </optgroup>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium mb-2 text-gray-300">
              开始日期（可选）
            </label>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="w-full px-4 py-2 bg-gray-600 border border-gray-500 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-white"
            />
          </div>

          <div>
            <label className="block text-sm font-medium mb-2 text-gray-300">
              结束日期（可选）
            </label>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="w-full px-4 py-2 bg-gray-600 border border-gray-500 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-white"
            />
          </div>
        </div>

        {/* 检查选项 */}
        <div className="space-y-2">
          <label className="block text-sm font-medium mb-2 text-gray-300">检查选项</label>
          <div className="flex flex-wrap gap-4">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={checkDuplicates}
                onChange={(e) => setCheckDuplicates(e.target.checked)}
                className="mr-2 w-4 h-4"
              />
              <span className="text-gray-300">检查重复数据</span>
            </label>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={checkMissingDates}
                onChange={(e) => setCheckMissingDates(e.target.checked)}
                className="mr-2 w-4 h-4"
              />
              <span className="text-gray-300">检查缺失日期</span>
            </label>
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={checkDataQuality}
                onChange={(e) => setCheckDataQuality(e.target.checked)}
                className="mr-2 w-4 h-4"
              />
              <span className="text-gray-300">检查数据质量</span>
            </label>
          </div>
        </div>

        <div className="flex flex-col gap-4">
          <button
            onClick={runCheck}
            disabled={loading}
            className="w-full px-6 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium rounded-lg transition-colors"
          >
            {loading ? '检查中...' : '开始检查'}
          </button>
          
          {result && (
            <div className="flex flex-wrap gap-2">
              <div className="flex items-center gap-2">
                <select
                  value={reportFormat}
                  onChange={(e) => setReportFormat(e.target.value)}
                  className="px-3 py-2 bg-gray-600 border border-gray-500 rounded-lg text-white text-sm"
                >
                  <option value="text">文本</option>
                  <option value="json">JSON</option>
                  <option value="html">HTML</option>
                  <option value="markdown">Markdown</option>
                </select>
                <button
                  onClick={generateReport}
                  disabled={generatingReport}
                  className="px-6 py-3 bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium rounded-lg transition-colors"
                >
                  {generatingReport ? '生成中...' : '生成完整报告'}
                </button>
              </div>
              
              {(result.symbols_with_issues.length > 0 || result.summary.empty_tables > 0) && (
                <>
                  <button
                    onClick={recheckProblematicSymbols}
                    disabled={rechecking}
                    className="px-6 py-3 bg-orange-600 hover:bg-orange-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium rounded-lg transition-colors"
                  >
                    {rechecking ? '复检中...' : '复检（对比交易所API）'}
                  </button>
                  <button
                    onClick={generateDownloadScript}
                    disabled={downloading}
                    className="px-6 py-3 bg-green-600 hover:bg-green-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium rounded-lg transition-colors"
                  >
                    生成下载脚本
                  </button>
                  <button
                    onClick={downloadMissingData}
                    disabled={downloading}
                    className="px-6 py-3 bg-purple-600 hover:bg-purple-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white font-medium rounded-lg transition-colors"
                  >
                    {downloading ? '下载中...' : '自动下载缺失数据'}
                  </button>
                </>
              )}
            </div>
          )}
        </div>
      </div>

      {/* 错误提示 */}
      {error && (
        <div className="bg-red-900/50 border border-red-500 rounded-lg p-4 text-red-200">
          {error}
        </div>
      )}

      {/* 检查结果 */}
      {result && (
        <div className="space-y-4">
          {/* 总结 */}
          <div className="bg-gray-700/50 rounded-lg p-6">
            <h3 className="text-xl font-bold mb-4 text-white">检查总结</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">总交易对数</div>
                <div className="text-2xl font-bold text-white">{result.total_symbols}</div>
              </div>
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">已检查</div>
                <div className="text-2xl font-bold text-white">{result.checked_symbols}</div>
              </div>
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">有问题的交易对</div>
                <div className="text-2xl font-bold text-red-400">{result.symbols_with_issues.length}</div>
              </div>
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">空表数量</div>
                <div className="text-2xl font-bold text-yellow-400">{result.summary.empty_tables}</div>
              </div>
            </div>

            <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">重复数据总数</div>
                <div className="text-xl font-bold text-orange-400">{result.summary.duplicates}</div>
              </div>
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">缺失日期总数</div>
                <div className="text-xl font-bold text-yellow-400">{result.summary.missing_dates}</div>
              </div>
              <div className="bg-gray-600/50 rounded p-4">
                <div className="text-gray-400 text-sm">数据质量问题总数</div>
                <div className="text-xl font-bold text-red-400">{result.summary.data_quality_issues}</div>
              </div>
            </div>
          </div>

          {/* 复检结果 */}
          {recheckResult && (
            <div className="bg-gray-700/50 rounded-lg p-6">
              <h3 className="text-xl font-bold mb-4 text-white">复检结果（对比交易所API）</h3>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4">
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">总复检数</div>
                  <div className="text-xl font-bold text-white">{recheckResult.total_rechecked}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">交易所API问题</div>
                  <div className="text-xl font-bold text-orange-400">{recheckResult.exchange_api_issues?.length || 0}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">本地数据问题</div>
                  <div className="text-xl font-bold text-red-400">{recheckResult.local_data_issues?.length || 0}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">两边都有问题</div>
                  <div className="text-xl font-bold text-yellow-400">{recheckResult.both_issues?.length || 0}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">可重新下载修复</div>
                  <div className="text-xl font-bold text-green-400">{recheckResult.fixed_by_redownload?.length || 0}</div>
                </div>
              </div>
              
              {/* 详细复检结果 */}
              {recheckResult.details && Object.keys(recheckResult.details).length > 0 && (
                <div className="mt-4 space-y-2 max-h-96 overflow-y-auto">
                  {Object.entries(recheckResult.details).map(([symbol, detail]: [string, any]) => (
                    <div
                      key={symbol}
                      className="border rounded-lg p-4 bg-gray-600/30"
                    >
                      <div className="font-bold text-white mb-2">{symbol}</div>
                      {detail.conclusion && (
                        <div className={`mb-2 p-2 rounded ${
                          detail.conclusion.includes('交易所API') ? 'bg-orange-900/30 text-orange-300' :
                          detail.conclusion.includes('本地数据') ? 'bg-red-900/30 text-red-300' :
                          detail.conclusion.includes('重新下载') ? 'bg-green-900/30 text-green-300' :
                          'bg-gray-900/30 text-gray-300'
                        }`}>
                          结论: {detail.conclusion}
                        </div>
                      )}
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <div className="text-gray-400">本地数据:</div>
                          <div className="text-white">
                            记录数: {detail.local_data?.record_count || 0} | 
                            重复: {detail.local_data?.duplicates || 0} | 
                            空值: {detail.local_data?.null_counts ? (Object.values(detail.local_data.null_counts) as number[]).reduce((a, b) => a + b, 0) : 0}
                          </div>
                        </div>
                        <div>
                          <div className="text-gray-400">交易所数据:</div>
                          <div className="text-white">
                            记录数: {detail.exchange_data?.record_count || 0} | 
                            重复: {detail.exchange_data?.duplicates || 0} | 
                            空值: {detail.exchange_data?.null_counts ? (Object.values(detail.exchange_data.null_counts) as number[]).reduce((a, b) => a + b, 0) : 0}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* 下载统计 */}
          {downloadStats && (
            <div className="bg-gray-700/50 rounded-lg p-6">
              <h3 className="text-xl font-bold mb-4 text-white">下载统计</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">空表下载</div>
                  <div className="text-xl font-bold text-green-400">{downloadStats.empty_tables_downloaded}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">缺失日期下载</div>
                  <div className="text-xl font-bold text-green-400">{downloadStats.missing_dates_downloaded}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">成功</div>
                  <div className="text-xl font-bold text-green-400">{downloadStats.success?.length || 0}</div>
                </div>
                <div className="bg-gray-600/50 rounded p-4">
                  <div className="text-gray-400 text-sm">失败</div>
                  <div className="text-xl font-bold text-red-400">{downloadStats.failed?.length || 0}</div>
                </div>
              </div>
              {downloadStats.failed && downloadStats.failed.length > 0 && (
                <div className="mt-4 text-red-400">
                  失败的交易对: {downloadStats.failed.join(', ')}
                </div>
              )}
            </div>
          )}

          {/* 详细结果 */}
          <div className="bg-gray-700/50 rounded-lg p-6">
            <h3 className="text-xl font-bold mb-4 text-white">详细结果</h3>
            <div className="space-y-2 max-h-96 overflow-y-auto">
              {Object.entries(result.details).map(([symbol, details]) => (
                <div
                  key={symbol}
                  className={`border rounded-lg p-4 ${
                    details.issues.length > 0
                      ? 'border-red-500 bg-red-900/20'
                      : 'border-green-500 bg-green-900/20'
                  }`}
                >
                  <div
                    className="flex items-center justify-between cursor-pointer"
                    onClick={() => toggleSymbol(symbol)}
                  >
                    <div className="flex items-center space-x-2">
                      <span className="text-lg font-bold text-white">{symbol}</span>
                      {details.issues.length === 0 ? (
                        <span className="text-green-400">✅</span>
                      ) : (
                        <span className="text-red-400">⚠️</span>
                      )}
                      <span className="text-gray-400 text-sm">
                        ({details.record_count} 条记录)
                      </span>
                    </div>
                    <span className="text-gray-400">
                      {expandedSymbols.has(symbol) ? '▼' : '▶'}
                    </span>
                  </div>

                  {expandedSymbols.has(symbol) && (
                    <div className="mt-3 space-y-2 text-sm">
                      {details.date_range && (
                        <div className="text-gray-300">
                          日期范围: {details.date_range.start} 至 {details.date_range.end} ({details.date_range.days} 天)
                        </div>
                      )}
                      {details.issues.length > 0 ? (
                        <div className="space-y-1">
                          <div className="text-red-400 font-medium">问题列表:</div>
                          {details.issues.map((issue, idx) => (
                            <div key={idx} className="text-red-300 ml-4">• {issue}</div>
                          ))}
                          {details.duplicate_count > 0 && (
                            <div className="text-orange-300 ml-4">
                              • 重复数据: {details.duplicate_count} 条
                            </div>
                          )}
                          {details.missing_dates.length > 0 && (
                            <div className="text-yellow-300 ml-4">
                              • 缺失日期: {details.missing_dates.join(', ')}
                              {details.missing_dates.length >= 10 && ' ...'}
                            </div>
                          )}
                          {details.invalid_price_data && details.invalid_price_data.length > 0 && (
                            <div className="mt-3 ml-4">
                              <div className="text-red-400 font-medium mb-2">
                                价格数据不合理详情 ({details.invalid_price_data.length} 条):
                              </div>
                              <div className="space-y-2 max-h-60 overflow-y-auto">
                                {details.invalid_price_data.map((data, idx) => (
                                  <div key={idx} className="bg-red-900/30 rounded p-2 text-xs">
                                    <div className="font-medium text-red-300 mb-1">
                                      日期: {data.trade_date}
                                    </div>
                                    <div className="text-gray-300 grid grid-cols-2 gap-1">
                                      <div>开盘: {data.open?.toLocaleString() || 'N/A'}</div>
                                      <div>最高: {data.high?.toLocaleString() || 'N/A'}</div>
                                      <div>最低: {data.low?.toLocaleString() || 'N/A'}</div>
                                      <div>收盘: {data.close?.toLocaleString() || 'N/A'}</div>
                                    </div>
                                    <div className="text-red-400 mt-1 text-xs">
                                      问题: {data.issues.join(', ')}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      ) : (
                        <div className="text-green-400">数据完整性检查通过</div>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

