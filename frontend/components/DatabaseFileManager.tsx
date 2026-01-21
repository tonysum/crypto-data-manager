'use client'

import { useState } from 'react'
import { API_URLS } from '../lib/api-config'

const API_BASE_URL = API_URLS.data

export default function DatabaseFileManager() {
  const [downloadingDb, setDownloadingDb] = useState(false)
  const [uploadingDb, setUploadingDb] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  const handleDownloadDatabase = async () => {
    setDownloadingDb(true)
    setMessage(null)

    try {
      const response = await fetch(`${API_BASE_URL}/api/download-database`, {
        method: 'GET',
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: '下载失败' }))
        throw new Error(errorData.detail || '下载数据库文件失败')
      }

      // 获取文件名（从响应头或生成）
      const contentDisposition = response.headers.get('Content-Disposition')
      let filename = 'crypto_data.db'
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?(.+)"?/i)
        if (filenameMatch) {
          filename = filenameMatch[1]
        }
      }

      // 获取文件大小
      const contentLength = response.headers.get('Content-Length')
      const fileSize = contentLength ? parseInt(contentLength, 10) : 0

      // 创建 Blob 并下载
      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = filename
      document.body.appendChild(a)
      a.click()
      window.URL.revokeObjectURL(url)
      document.body.removeChild(a)

      const sizeMB = (fileSize / (1024 * 1024)).toFixed(2)
      setMessage({
        type: 'success',
        text: `数据库文件下载成功！文件名: ${filename}，大小: ${sizeMB} MB`
      })
    } catch (error) {
      setMessage({
        type: 'error',
        text: error instanceof Error ? error.message : '下载数据库文件失败'
      })
    } finally {
      setDownloadingDb(false)
    }
  }

  const handleUploadDatabase = async () => {
    if (!selectedFile) {
      setMessage({
        type: 'error',
        text: '请先选择要上传的文件'
      })
      return
    }

    setUploadingDb(true)
    setMessage(null)

    try {
      const formData = new FormData()
      formData.append('file', selectedFile)

      const response = await fetch(`${API_BASE_URL}/api/upload-database`, {
        method: 'POST',
        body: formData,
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: '上传失败' }))
        throw new Error(errorData.detail || '上传数据库文件失败')
      }

      const result = await response.json()
      setMessage({
        type: 'success',
        text: `数据库文件上传成功！文件名: ${result.filename}，大小: ${result.size_mb} MB，保存路径: ${result.path}`
      })
      setSelectedFile(null)
      // 清空文件选择
      const fileInput = document.getElementById('db-upload-input') as HTMLInputElement
      if (fileInput) {
        fileInput.value = ''
      }
    } catch (error) {
      setMessage({
        type: 'error',
        text: error instanceof Error ? error.message : '上传数据库文件失败'
      })
    } finally {
      setUploadingDb(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold mb-2">数据库文件管理</h2>
        <p className="text-gray-400">上传和下载数据库文件 (crypto_data.db)</p>
      </div>

      {message && (
        <div
          className={`p-4 rounded-lg ${
            message.type === 'success'
              ? 'bg-green-500/20 text-green-400 border border-green-500/50'
              : 'bg-red-500/20 text-red-400 border border-red-500/50'
          }`}
        >
          {message.text}
        </div>
      )}

      <div className="grid md:grid-cols-2 gap-6">
        {/* 下载数据库文件 */}
        <div className="p-6 bg-gray-800/50 rounded-lg border border-gray-700">
          <div className="flex items-center mb-4">
            <span className="text-3xl mr-3">📥</span>
            <div>
              <h3 className="text-lg font-semibold">下载数据库文件</h3>
              <p className="text-sm text-gray-400">从服务器下载完整的数据库文件到本地</p>
            </div>
          </div>
          <div className="mt-6">
            <button
              type="button"
              onClick={handleDownloadDatabase}
              disabled={downloadingDb}
              className={`w-full px-6 py-3 rounded-lg font-medium transition-colors ${
                downloadingDb
                  ? 'bg-gray-600 cursor-not-allowed'
                  : 'bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700'
              }`}
            >
              {downloadingDb ? '下载中...' : '📥 下载数据库文件'}
            </button>
          </div>
          <div className="mt-4 text-xs text-gray-500">
            <p>• 文件将下载到浏览器的默认下载文件夹</p>
            <p>• 文件名格式: crypto_data_YYYYMMDD_HHMMSS.db</p>
          </div>
        </div>

        {/* 上传数据库文件 */}
        <div className="p-6 bg-gray-800/50 rounded-lg border border-gray-700">
          <div className="flex items-center mb-4">
            <span className="text-3xl mr-3">📤</span>
            <div>
              <h3 className="text-lg font-semibold">上传数据库文件</h3>
              <p className="text-sm text-gray-400">上传数据库文件到服务器的 data/tmp 文件夹</p>
            </div>
          </div>
          <div className="mt-6 space-y-4">
            <div>
              <label className="block mb-2">
                <input
                  type="file"
                  accept=".db"
                  onChange={(e) => {
                    const file = e.target.files?.[0]
                    if (file) {
                      if (!file.name.endsWith('.db')) {
                        setMessage({
                          type: 'error',
                          text: '只能上传 .db 文件'
                        })
                        return
                      }
                      setSelectedFile(file)
                      setMessage(null)
                    }
                  }}
                  className="hidden"
                  id="db-upload-input"
                />
                <div className="flex items-center space-x-2">
                  <button
                    type="button"
                    onClick={() => document.getElementById('db-upload-input')?.click()}
                    className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors"
                  >
                    选择文件
                  </button>
                  {selectedFile && (
                    <div className="flex-1">
                      <p className="text-sm text-gray-300 truncate" title={selectedFile.name}>
                        {selectedFile.name}
                      </p>
                      <p className="text-xs text-gray-500">
                        {(selectedFile.size / (1024 * 1024)).toFixed(2)} MB
                      </p>
                    </div>
                  )}
                </div>
              </label>
            </div>
            <button
              type="button"
              onClick={handleUploadDatabase}
              disabled={!selectedFile || uploadingDb}
              className={`w-full px-6 py-3 rounded-lg font-medium transition-colors ${
                !selectedFile || uploadingDb
                  ? 'bg-gray-600 cursor-not-allowed'
                  : 'bg-gradient-to-r from-blue-500 to-indigo-600 hover:from-blue-600 hover:to-indigo-700'
              }`}
            >
              {uploadingDb ? '上传中...' : '📤 上传数据库文件'}
            </button>
          </div>
          <div className="mt-4 text-xs text-gray-500">
            <p>• 只支持 .db 格式文件</p>
            <p>• 文件将保存到: data/tmp/</p>
            <p>• 文件名格式: 原文件名_YYYYMMDD_HHMMSS.db</p>
          </div>
        </div>
      </div>

      {/* 使用说明 */}
      <div className="p-4 bg-blue-500/10 border border-blue-500/30 rounded-lg">
        <h4 className="text-sm font-semibold text-blue-400 mb-2">💡 使用说明</h4>
        <ul className="text-xs text-gray-400 space-y-1">
          <li>• <strong>下载</strong>: 从服务器下载当前使用的数据库文件，可用于备份或迁移</li>
          <li>• <strong>上传</strong>: 将数据库文件上传到服务器的临时文件夹，可用于恢复或替换数据库</li>
          <li>• 上传的文件保存在 data/tmp/ 目录，不会自动替换当前使用的数据库</li>
          <li>• 如需替换当前数据库，请手动将上传的文件移动到 data/ 目录并重命名为 crypto_data.db</li>
        </ul>
      </div>
    </div>
  )
}
