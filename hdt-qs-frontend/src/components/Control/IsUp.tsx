import React, { useEffect, useState } from 'react'
import { Chip } from '@mui/material'
import { mergeClasses } from 'common/react-utils'
import { useFastAPI } from 'common/api'
import config from 'common/config'

import s from './IsUp.module.scss'

export default function IsUp () {
  const [isUp, setIsUp] = useState(false)

  const {
    setRequest,
    error,
    success,
    loading
  } = useFastAPI()

  // Make the request on mount
  useEffect(() => {
    setRequest(fetch(`${config.apiBase}/api/endpoint/`))
  }, [setRequest])

  // Set isUp on success
  useEffect(() => {
    if (success) setIsUp(true)
  }, [success])
  // Set isUp on error
  useEffect(() => {
    if (error) setIsUp(false)
  }, [error])
  // Set isUp when loading
  useEffect(() => {
    if (loading) setIsUp(false)
  }, [loading])

  return (
    <Chip
      label={loading ? 'Loading...' : (isUp ? 'Connected' : 'Disconnected')}
      icon={<GreenDot on={isUp} />}
    />
  )
}

interface GreenDotProps {
  on: boolean;
}

function GreenDot (props: GreenDotProps) {
  const { on } = props
  return (
    <div className={mergeClasses(s.greenDot, on && s.on)}>
      <div className={s.diffusion} />
    </div>
  )
}
