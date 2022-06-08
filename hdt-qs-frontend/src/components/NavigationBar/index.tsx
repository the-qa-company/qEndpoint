import React, { useState } from 'react'
import { Divider, List, Typography } from '@mui/material'
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow'
import InputIcon from '@mui/icons-material/Input'
import InfoIcon from '@mui/icons-material/Info'
import { useNavigate } from 'react-router'
import { Link } from 'react-router-dom'

import Drawer from './Drawer'
import Item from './Item'

import s from './index.module.scss'

import logo from 'res/logo/the-qa-company-notext.png'

export const retractedWidth = 50
export const openWidth = 250

type TabDef = {
  type: 'redirect',
  path: string,
  name: string,
  icon: React.ReactElement,
} | {
  type: 'regular',
  matcher: (path: string) => boolean,
  path: string,
  name: string,
  icon: React.ReactElement,
}

// Define tabs here
const tabs: TabDef[] = [
  {
    name: 'Endpoint',
    type: 'regular',
    matcher: (path) => path.length === 0 || path === '/',
    path: '/',
    icon: <InputIcon />
  },
  {
    name: 'About',
    type: 'redirect',
    path: 'https://qanswer.ai/',
    icon: <InfoIcon />
  }
]

export default function NavigationBar () {
  const navigate = useNavigate()
  const [open, setOpen] = useState(true)
  return (
    <Drawer
      open={open}
      className={s.drawer}
    >
      <div>
        <Link to='/' className={s.top}>
          <img
            src={logo}
            alt='The QA Conpany logo'
            className={s.logo}
            style={{ width: retractedWidth }}
            onClick={() => navigate('/')}
          />
          <Typography className={s.title} variant='h6' component='h1'>qEndpoint</Typography>
        </Link>
      </div>
      <Divider />
      <List sx={{ flexGrow: 1 }}>
        {
          tabs.map((tab, index) => (
            <Item
              key={index}
              open={open}
              icon={tab.icon}
              name={tab.name}
              showName
              selected={tab.type === 'redirect' ? false : tab.matcher(window.location.pathname)}
              onClick={() => tab.type === 'redirect' ? window.open(tab.path, '_blank') : navigate(tab.path)}
            />
          ))
        }
      </List>
      <Divider />
      <List>
        <Item
          icon={
            <DoubleArrowIcon
              sx={{ transform: `rotate(${open ? 180 : 0}deg)`, transition: 'transform 0.25s ease-in-out' }}
            />
          }
          name='Collapse'
          open={open}
          onClick={() => setOpen(o => !o)}
        />
      </List>
    </Drawer>
  )
}
