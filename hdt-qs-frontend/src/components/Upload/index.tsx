import React from 'react'
import { Typography, useTheme } from '@mui/material'
import Dnd from './Dnd'

import s from './index.module.scss'

export default function Upload () {
  const theme = useTheme()

  return (
    <div className={s.container}>
      <div className={s.centered}>
        <Typography className={s.centered} sx={{ color: theme.palette.text.secondary, marginTop: '30px' }} variant='h4'>Upload</Typography>
        <div style={{ color: theme.palette.text.secondary }}>
          Add files to the HDT store.
        </div>
      </div>
      <div className={s.body}>
        <Dnd />
      </div>
    </div>
  )
}
