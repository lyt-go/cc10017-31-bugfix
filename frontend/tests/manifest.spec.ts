import { expect, test } from '@playwright/test'

/**
 * 合并批次清单跨模块链路：
 * 流式补数页追加文件 → 合并清理页生成批次、模拟中断并续做 →
 * 快照统计不重复计数 → 下载清单保留书签与版本 → 刷新补数页与工作台同步。
 */

let runSeq = 0
function uniqueFile(prefix: string): string {
  runSeq += 1
  return `warehouse/orders/backfill/${prefix}-${Date.now()}-${runSeq}.parquet`
}

test('合并批次清单：中断续做、补数追加、快照去重与下载书签', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', (error) => browserErrors.push(error.message))
  page.on('response', (response) => {
    if (response.url().includes('/api/') && response.status() >= 400) {
      browserErrors.push(`${response.status()} ${response.url()}`)
    }
  })

  // ── 流式补数页：追加补数文件（先读清单，再进入待入批队列）──
  await page.goto('/backfill_stream')
  await expect(page.getByTestId('manifest-panel')).toBeVisible({ timeout: 15_000 })
  // 清掉上一轮遗留的未完成批次，避免新文件被旧批次挡住。
  await page.getByTestId('manifest-resume-all').click()
  await page.waitForTimeout(300)
  for (const name of ['e2e-a', 'e2e-b', 'e2e-c', 'e2e-d']) {
    await page.getByTestId('manifest-intake-input').fill(uniqueFile(name))
    await page.getByTestId('manifest-intake-append').click()
    await expect(page.getByTestId('manifest-message')).toContainText('新补数文件已接收')
  }
  await expect(page.getByTestId('manifest-duplicate-count')).toHaveText('0')

  // ── 合并清理工作台：续做遗留批次后，重新读取策略生成批次 ──
  await page.goto('/compact_policy')
  await expect(page.getByTestId('manifest-panel')).toBeVisible({ timeout: 15_000 })
  await page.getByTestId('manifest-resume-all').click()
  await page.waitForTimeout(300)

  await page.getByTestId('manifest-generate').click()
  await expect(page.getByTestId('manifest-message')).toContainText('已生成')
  await expect(page.getByTestId('manifest-batch-count')).not.toHaveText('0')
  expect(Number(await page.getByTestId('manifest-unfinished-count').innerText())).toBeGreaterThan(0)

  // 中断第一个未完成批次：清单保留已合并文件、目标文件与续做边界。
  const pendingRow = page.locator('tbody tr[data-testid^="manifest-batch-"][data-status="待合并"]').first()
  const batchNo = (await pendingRow.locator('td').first().innerText()).trim()
  const batchRow = page.getByTestId(`manifest-batch-${batchNo}`)
  await page.getByTestId(`manifest-interrupt-${batchNo}`).click()
  await expect(batchRow).toHaveAttribute('data-status', '中断')
  const boundary = await batchRow.locator('td').nth(6).innerText()
  expect(boundary).not.toBe('—')
  expect(Number(await batchRow.locator('td').nth(4).innerText())).toBeGreaterThan(0)

  // 继续合并：只续做未完成批次，已合并文件不会被再次合并。
  await page.getByTestId(`manifest-resume-${batchNo}`).click()
  await expect(batchRow).toHaveAttribute('data-status', '已完成')
  expect(Number(await batchRow.locator('td').nth(4).innerText())).toBeGreaterThan(0)

  // 快照统计没有同一文件重复计数。
  await expect(page.getByTestId('manifest-duplicate-count')).toHaveText('0')
  expect(Number(await page.getByTestId('manifest-snapshot-files').innerText())).toBeGreaterThan(0)

  // 下载清单：保留下载书签与当前版本。
  const versionBefore = await page.getByTestId('manifest-version').innerText()
  const downloadPromise = page.waitForEvent('download')
  await page.getByTestId('manifest-export').click()
  const download = await downloadPromise
  expect(download.suggestedFilename()).toBe('compact_manifest_orders.csv')
  await expect(page.getByTestId('manifest-message')).toContainText('书签')
  await expect(page.getByTestId('manifest-last-download')).toContainText(`v${versionBefore.replace('v', '')}`)

  // ── 流式补数页：追加一个文件，重复计数仍为 0 ──
  await page.goto('/backfill_stream')
  await expect(page.getByTestId('manifest-panel')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByTestId('manifest-version')).toHaveText(versionBefore)

  await page.getByTestId('manifest-intake-input').fill(uniqueFile('e2e-after'))
  await page.getByTestId('manifest-intake-append').click()
  await expect(page.getByTestId('manifest-message')).toBeVisible()
  expect(await page.getByTestId('manifest-intake-decision').innerText()).not.toBe('判定：—')
  await expect(page.getByTestId('manifest-duplicate-count')).toHaveText('0')

  // 刷新补数页后，清单版本与合并清理工作台保持一致。
  const versionAfterAppend = await page.getByTestId('manifest-version').innerText()
  await page.reload()
  await expect(page.getByTestId('manifest-panel')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByTestId('manifest-version')).toHaveText(versionAfterAppend)

  await page.goto('/compact_policy')
  await expect(page.getByTestId('manifest-panel')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByTestId('manifest-version')).toHaveText(versionAfterAppend)
  await expect(page.getByTestId('manifest-duplicate-count')).toHaveText('0')

  expect(browserErrors).toEqual([])
})
