import { useEffect, useMemo, useState } from 'react'
import { isOk } from 'common/http'
import { useSnackbar } from 'notistack'

export const useAPI = (prom: Promise<Response> | undefined) => {
  const [state, setState] = useState({
    error: false,
    loading: prom !== undefined,
    res: undefined as undefined | Response,
    success: false,
    finished: false
  })

  useEffect(() => {
    if (prom !== undefined) {
      setState((s) => ({
        ...s, loading: true, res: undefined, error: false, success: false, finished: false
      }))
      let shouldIgnore = false
      prom
        .then((res) => {
          if (shouldIgnore) return
          setState((s) => ({
            ...s,
            error: !isOk(res.status),
            loading: false,
            res,
            success: isOk(res.status),
            finished: true
          }))
        })
        .catch((err) => {
          if (shouldIgnore) return
          setState((s) => ({
            ...s,
            error: true,
            loading: false,
            res: err.response,
            success: false,
            finished: true
          }))
        })
      return () => {
        shouldIgnore = true
      }
    }
    return undefined
  }, [prom])

  return state
}

export interface FastOptions {
  autoNotify?: boolean;
  label?: string;
  errorMsg?: string;
}

export const useFastAPI = (options?: FastOptions) => {
  const { autoNotify = false, label, errorMsg } = options || {}

  const { enqueueSnackbar } = useSnackbar()

  const [prom, setProm] = useState<Promise<Response> | undefined>(undefined)
  const useAPIData = useAPI(prom)

  // Auto-notify on error
  useEffect(() => {
    if (autoNotify && useAPIData.error) {
      let msg = (label ? (label + ': ') : '') + (useAPIData.res?.statusText || 'Unknown error')
      if (errorMsg) msg = errorMsg
      enqueueSnackbar(msg, { variant: 'error' })
    }
  }, [autoNotify, enqueueSnackbar, errorMsg, label, useAPIData.error, useAPIData.res?.statusText])

  return useMemo(() => ({
    ...useAPIData,
    setRequest: setProm,
    rawRequest: prom
  }), [prom, useAPIData])
}
