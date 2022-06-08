import React, { useEffect, useRef, useState } from 'react'
import { useSyncRef } from 'common/react-hooks'
import { useNavigate } from 'react-router-dom'
import { TextField, Typography, useTheme } from '@mui/material'

import Yasgui from '@triply/yasgui'
import '@triply/yasgui/build/yasgui.min.css'

import s from './index.module.scss'

interface ExportedYasguiTab {
  query: string;
  name: string;
}

const exportYasguiToParams = (instance: Yasgui): URLSearchParams => {
  const params = new URLSearchParams()
  try {
    const tabs = Object.values(instance._tabs)
      .map((tab): ExportedYasguiTab => ({ query: tab.getQuery(), name: tab.getName() }))
    params.set('tabs', JSON.stringify(tabs))
  } catch (e) {
    console.warn(e)
  }
  return params
}

export default function SparqlEndpoint () {
  const navigate = useNavigate()
  const theme = useTheme()

  const yasguiRef = useRef<Yasgui>()
  const yasguiDivRef = useRef<HTMLDivElement>(null)
  const [timeoutValue, setTimeoutValue] = useState('5')

  const timeoutRef = useRef<typeof timeoutValue>()
  timeoutRef.current = timeoutValue

  // Called whenever the yasgui instance is updated and needs to be saved
  const onYasguiParamsChange = useSyncRef((params: URLSearchParams) => {
    const newParams = new URLSearchParams(window.location.search)
    params.forEach((value, key) => {
      newParams.set(key, value)
    })
    navigate({
      search: newParams.toString()
    })
  })

  // Init Yasgui
  useEffect(() => {
    if (yasguiDivRef.current === null) return undefined
    yasguiDivRef.current.innerHTML = ''

    // Construct Yasgui
    const yasgui = new Yasgui(yasguiDivRef.current, {
      requestConfig: {
        endpoint: 'http://localhost:1234/api/endpoint/sparql',
        headers: () => ({
          timeout: timeoutRef.current || ''
        }),
        method: 'GET'
      },
      copyEndpointOnNewTab: false,
      persistencyExpire: 1 / 1000000 // No persistency
    })

    // Store reference to Yasgui for later use
    yasguiRef.current = yasgui

    // Load from params
    const params = new URLSearchParams(window.location.search)
    if (params.get('tabs')) {
      for (let tab = yasgui.getTab(); tab !== undefined; tab = yasgui.getTab()) {
        tab.close()
      }
      const tabs = JSON.parse(params.get('tabs') || '[]') as ExportedYasguiTab[]
      tabs.forEach((tab) => {
        yasgui.addTab(
          true,
          {
            ...(yasgui.config as any),
            name: tab.name,
            yasqe: { value: tab.query }
          }
        )
      })
    }

    // Listen for changes in the yasgui instance
    const onChangeDetected = () => onYasguiParamsChange
      .current(exportYasguiToParams(yasgui))
    // Listen for changes on existing tabs
    Object.values(yasgui._tabs).forEach((tab) => {
      (tab.getYasqe() as any)?.on('change', () => onChangeDetected())
    })
    // Listen for changes on new tabs
    yasgui.on('tabAdd', (instance, tabId) => {
      // Set a timeout to let the div render
      setTimeout(() => {
        onChangeDetected();
        (instance.getTab(tabId)?.getYasqe() as any)?.on('change', () => onChangeDetected())
      }, 0)
    })
    // Listen for other changes
    yasgui.on('tabChange', () => onChangeDetected())
    yasgui.on('tabClose', () => onChangeDetected())
    yasgui.on('tabOrderChanged', () => onChangeDetected())
    yasgui.on('query', () => onChangeDetected())
    return () => {
      yasgui.removeAllListeners()
      yasgui.destroy()
    }
  }, [onYasguiParamsChange, yasguiDivRef])

  return (
    <div className={s.container}>
      <div className={s.centered}>
        <Typography className={s.centered} sx={{ color: theme.palette.text.secondary, marginTop: '30px' }} variant='h4'>Query Service</Typography>
        <div style={{ color: theme.palette.text.secondary }}>
          This interface is a SPARQL endpoint. Just type your query and execute it.
        </div>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <p style={{ color: theme.palette.text.secondary, fontWeight: 'bold' }}>Timeout (in seconds):</p>
        <TextField
          value={timeoutValue}
          type='number'
          size='small'
          inputProps={{
            min: 0
          }}
          onChange={event => setTimeoutValue(event.currentTarget.value)}
        />
      </div>
      <div ref={yasguiDivRef} className={s.yasgui} />
    </div>
  )
}
