import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { Button, Tooltip, Typography, useTheme } from '@mui/material'
import { LoadingButton } from '@mui/lab'
import CallMergeIcon from '@mui/icons-material/CallMerge'
import ListIcon from '@mui/icons-material/List'
import FeedIcon from '@mui/icons-material/Feed'
import { useFastAPI } from 'common/api'
import config from 'common/config'
import IsUp from './IsUp'
import useAsyncEffect from 'use-async-effect'
import { useSnackbar } from 'notistack'

import s from './index.module.scss'

export default function Control () {
  const theme = useTheme()
  const { enqueueSnackbar } = useSnackbar()

  const [isMerging, setIsMerging] = useState<boolean>()
  const [isMergingTooltipVisible, setIsMergingTooltipVisible] = useState(false)
  const [hasIndex, setHasIndex] = useState<boolean>()

  // hasIndex request
  const hasIndexReq = useFastAPI({ autoNotify: true, errorMsg: 'Impossible to know if the endpoint has an index' })
  const {
    setRequest: setHasIndexRequest
  } = hasIndexReq

  const checkHasIndex = useCallback(() => {
    setHasIndexRequest(fetch(`${config.apiBase}/api/endpoint/has_index`))
  }, [setHasIndexRequest])

  // Make hasIndex request on mount
  useEffect(() => {
    checkHasIndex()
  }, [checkHasIndex])

  // Read hasIndex response
  useAsyncEffect(async () => {
    if (hasIndexReq.success && hasIndexReq.res !== undefined) {
      const parsed = await hasIndexReq.res.json()
      if (typeof parsed.hasLuceneIndex === 'boolean') {
        setHasIndex(parsed.hasLuceneIndex)
      }
    }
  }, [hasIndexReq.success, hasIndexReq.res])

  // isMerging request
  const isMergingReq = useFastAPI({ autoNotify: true, errorMsg: 'Impossible to know whether the endpoint is merging or not' })
  const {
    setRequest: setIsMergingReq
  } = isMergingReq

  const checkIsMerging = useCallback(() => {
    setIsMergingReq(fetch(`${config.apiBase}/api/endpoint/is_merging`))
  }, [setIsMergingReq])

  // Make isMerging request on mount
  useEffect(() => {
    checkIsMerging()
  }, [checkIsMerging])

  // Read isMerging response
  useAsyncEffect(async () => {
    if (isMergingReq.success && isMergingReq.res !== undefined) {
      const parsed = await isMergingReq.res.json()
      setIsMerging(parsed.merging)
    }
  }, [isMergingReq.success, isMergingReq.res])

  // Hide tooltip when server says it's not merging or is loading
  useEffect(() => {
    if (isMerging === false || isMerging === undefined) {
      setIsMergingTooltipVisible(false)
    }
  }, [isMerging])

  // Merge request
  const mergeReq = useFastAPI({ autoNotify: true, label: 'Merge request' })

  // Notify on merge success
  useEffect(() => {
    if (mergeReq.success) {
      enqueueSnackbar('Merge request sent', { variant: 'success' })
    }
  }, [enqueueSnackbar, mergeReq.success])

  // Update UI when merge request is finished
  useEffect(() => {
    if (mergeReq.finished) {
      checkIsMerging()
    }
  }, [checkIsMerging, mergeReq.finished])

  // Index request
  const indexReq = useFastAPI({ autoNotify: true, label: 'Index request' })

  const index = () => {
    indexReq.setRequest(fetch(`${config.apiBase}/api/endpoint/reindex`))
  }

  // Notify user when indexing is finished successfully
  useEffect(() => {
    if (indexReq.success) {
      enqueueSnackbar('Re-indexed successfully', { variant: 'success' })
    }
  }, [enqueueSnackbar, indexReq.success])

  const isMergingBtnLoading = useMemo(() => {
    const isRequestLaunched = isMergingReq.rawRequest !== undefined
    if (isMergingReq.loading || !isRequestLaunched) return true
    if (isMerging === true) return true
    return false
  }, [isMerging, isMergingReq.loading, isMergingReq.rawRequest])

  const dontShowIndexButton = !indexReq.loading && (hasIndexReq.loading || (hasIndexReq.success && hasIndex === false))

  const openLogs = () => {
    window.open(config.apiBase + '/actuator/logfile', '_blank')
  }

  return (
    <div className={s.container}>
      <div className={s.centered}>
        <Typography className={s.centered} sx={{ color: theme.palette.text.secondary, marginTop: '30px' }} variant='h4'>Control</Typography>
        <div style={{ color: theme.palette.text.secondary }}>
          This interface is a control panel for the HDT store.
        </div>
      </div>
      <div className={s.body}>
        <IsUp />
        <div className={s.buttons}>
          <Tooltip
            title='Merging...'
            open={isMergingTooltipVisible}
            onOpen={() => isMerging && setIsMergingTooltipVisible(true)}
            onClose={() => setIsMergingTooltipVisible(false)}
          >
            <LoadingButton
              variant='contained'
              startIcon={<CallMergeIcon />}
              loading={isMergingBtnLoading}
              onClick={() => mergeReq.setRequest(fetch(`${config.apiBase}/api/endpoint/merge`))}
            >
              Merge
            </LoadingButton>
          </Tooltip>

          {!dontShowIndexButton && (
            <LoadingButton
              variant='contained'
              startIcon={<ListIcon />}
              loading={indexReq.loading}
              onClick={index}
            >
              Re-Index
            </LoadingButton>
          )}

          <Button
            variant='contained'
            startIcon={<FeedIcon />}
            onClick={openLogs}
          >
            Open logs
          </Button>
        </div>
      </div>
    </div>
  )
}
