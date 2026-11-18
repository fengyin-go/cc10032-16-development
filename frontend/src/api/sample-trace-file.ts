/** 序号16 导入导出文件读取工具：把选中的 JSON 文件解析为对象。 */
export function readJsonFile(file: File): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      try {
        const parsed = JSON.parse(String(reader.result))
        if (parsed && typeof parsed === 'object') {
          resolve(parsed as Record<string, unknown>)
        } else {
          reject(new Error('文件内容必须是 JSON 对象'))
        }
      } catch {
        reject(new Error('JSON 解析失败，请确认上传的是序号16导出文件'))
      }
    }
    reader.onerror = () => reject(new Error('文件读取失败'))
    reader.readAsText(file, 'utf-8')
  })
}
