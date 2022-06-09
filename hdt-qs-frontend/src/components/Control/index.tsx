import React, { useEffect, useMemo } from 'react'
import { Typography, useTheme } from '@mui/material'
import { LoadingButton } from '@mui/lab'
import CallMergeIcon from '@mui/icons-material/CallMerge'
import ListIcon from '@mui/icons-material/List'
import { useFastAPI } from 'common/api'
import config from 'common/config'
import IsUp from './IsUp'

import s from './index.module.scss'

export default function Control () {
  const theme = useTheme()

  // isMerging request
  const isMergingReq = useFastAPI({ autoNotify: true, errorMsg: 'Impossible to know whether the endpoint is merging or not' })
  const {
    setRequest: setIsMergingReq
  } = isMergingReq

  // Make isMerging request on mount
  useEffect(() => {
    setIsMergingReq(fetch(`${config.apiBase}/endpoint/is_merging`))
  }, [setIsMergingReq])

  // Merge request
  const mergeReq = useFastAPI({ autoNotify: true, label: 'Merge request' })

  console.log(JSON.stringify(mergeReq, null, 2))

  const isMergingBtnLoading = useMemo(() => {
    if (isMergingReq.loading || isMergingReq.rawRequest === undefined) return true
    // return isMergingReq.res
    return false
  }, [isMergingReq.loading, isMergingReq.rawRequest])

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
          <LoadingButton
            variant='contained'
            startIcon={<CallMergeIcon />}
            loading={isMergingBtnLoading}
            onClick={() => mergeReq.setRequest(fetch(`${config.apiBase}/endpoint/merge`))}
          >
            Merge
          </LoadingButton>

          <LoadingButton
            variant='contained'
            startIcon={<ListIcon />}
          >
            Re-Index
          </LoadingButton>
        </div>
      </div>
    </div>
  )
}
