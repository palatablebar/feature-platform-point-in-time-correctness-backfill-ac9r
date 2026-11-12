import { http } from './http'
import type { PageResult } from '../types/api'
import type { BackfillResultsPayload, BackfillTask } from '../types/backfill'
import type { MetaPayload } from '../types/catalogue'

export async function listBackfills(): Promise<PageResult<BackfillTask>> {
  const { data } = await http.get('/backfills')
  return data
}

export async function getBackfill(id: number): Promise<BackfillTask> {
  const { data } = await http.get(`/backfills/${id}`)
  return data
}

export async function createBackfill(payload: Record<string, unknown>): Promise<BackfillTask> {
  const { data } = await http.post('/backfills', payload)
  return data
}

export async function runBackfill(id: number): Promise<Record<string, unknown>> {
  const { data } = await http.post(`/backfills/${id}/run`)
  return data
}

export async function fetchBackfillResults(
  id: number,
  filters: { status?: string; conflictOnly?: boolean } = {},
): Promise<BackfillResultsPayload> {
  const params: Record<string, unknown> = {}
  if (filters.status) params.status = filters.status
  if (filters.conflictOnly) params.conflict_only = true
  const { data } = await http.get(`/backfills/${id}/results`, { params })
  return data
}

export async function fetchMeta(): Promise<MetaPayload> {
  const { data } = await http.get('/meta')
  return data
}
