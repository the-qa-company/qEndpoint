import React from 'react'
import { Drawer as MuiDrawer } from '@mui/material'

import { openWidth, retractedWidth } from '..'

import s from './index.module.scss'

interface Props extends React.ComponentProps<typeof MuiDrawer> {
  children?: React.ReactNode
  open: boolean;
}

export default function Drawer (props: Props) {
  const { children, open, ...otherProps } = props

  const getWidth = () => open ? openWidth : retractedWidth

  return (
    <MuiDrawer
      variant='permanent'
      anchor='left'
      {...otherProps}
      open={open}
      className={[s.drawer].concat(otherProps.className ?? []).join(' ')}
      sx={{
        ...otherProps.sx,
        width: getWidth(),
        '& .MuiDrawer-paper': {
          width: getWidth()
        }
      }}
    >
      {children}
    </MuiDrawer>
  )
}
