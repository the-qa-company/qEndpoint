import React from 'react'
import { Box } from '@mui/material'
import { Route, Routes } from 'react-router'
import NavigationBar from 'components/NavigationBar'
import SparqlEndpoint from 'components/SparqlEndpoint'
import Control from 'components/Control'
import Upload from 'components/Upload'

import s from './App.module.scss'

export default function App () {
  return (
    <Box className={s.container}>
      <NavigationBar />
      <Routes>
        <Route path='/' element={<SparqlEndpoint />} />
        <Route path='/upload' element={<Upload />} />
        <Route path='/control' element={<Control />} />
        <Route path='*' element={<div>404 - Page not found</div>} />
      </Routes>
    </Box>
  )
}
