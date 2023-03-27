import React, { useEffect, useState } from 'react'
import { Divider, List, Typography } from '@mui/material'
import DoubleArrowIcon from '@mui/icons-material/DoubleArrow'
import ArrowForwardIosIcon from '@mui/icons-material/ArrowForwardIos'
import InfoIcon from '@mui/icons-material/Info'
import ControlCameraIcon from '@mui/icons-material/ControlCamera'
import DriveFileRenameOutlineOutlinedIcon from '@mui/icons-material/DriveFileRenameOutlineOutlined'
import FileUploadIcon from '@mui/icons-material/FileUpload'
import { useNavigate } from 'react-router'
import { Link } from 'react-router-dom'
import config from 'common/config'

import Drawer from './Drawer'
import Item from './Item'

import s from './index.module.scss'

import logo from 'res/logo/the-qa-company-notext.png'
import * as localStorage from 'common/local-storage'

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
    icon: <ArrowForwardIosIcon />
  },
  {
    name: 'Upload',
    type: 'regular',
    matcher: (path) => path === '/upload' || path.startsWith('/upload/'),
    path: '/upload',
    icon: <FileUploadIcon />
  },
  {
    name: 'Prefixes',
    type: 'regular',
    matcher: (path) => path === '/prefixes' || path.startsWith('/prefixes/'),
    path: '/prefixes',
    icon: <DriveFileRenameOutlineOutlinedIcon />
  },
  {
    name: 'Control',
    type: 'regular',
    matcher: (path) => path === '/control' || path.startsWith('/control/'),
    path: '/control',
    icon: <ControlCameraIcon />
  },
  {
    name: 'About',
    type: 'redirect',
    path: config.aboutPage,
    icon: <InfoIcon />
  }
]

export default function NavigationBar () {
  const navigate = useNavigate()

  const [open, setOpen] = useState(localStorage.getItem('isNavigationBarOpen') ?? true)

  // Save open state in local storage
  useEffect(() => {
    localStorage.setItem('isNavigationBarOpen', open)
  }, [open])

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
          <Typography className={s.title} variant='h5' component='h1'>qEndpoint</Typography>
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
