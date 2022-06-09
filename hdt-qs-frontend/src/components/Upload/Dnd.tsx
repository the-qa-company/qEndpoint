import React from 'react'
import { Typography, useTheme } from '@mui/material'
import CloudUploadIcon from '@mui/icons-material/CloudUploadOutlined'

import s from './Dnd.module.scss'

export default function Dnd () {
  const theme = useTheme()

  const mainColor = theme.palette.text.secondary

  return (
    <div className={s.dnd} style={{ border: `2px dashed ${mainColor}` }}>
      <div className={s.center}>
        <CloudUploadIcon sx={{ color: mainColor, fontSize: 54 }} />
        <Typography variant='h6' component='h3' sx={{ color: mainColor }}>
          Drag and drop your files or click here
        </Typography>
      </div>
    </div>
  )
}
