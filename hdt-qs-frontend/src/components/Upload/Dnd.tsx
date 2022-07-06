import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Button, Chip, Typography, useTheme } from '@mui/material'
import { LoadingButton } from '@mui/lab'
import CloudUploadIcon from '@mui/icons-material/CloudUploadOutlined'
import ClearIcon from '@mui/icons-material/Clear'
import SendIcon from '@mui/icons-material/Send'
import { mergeClasses } from 'common/react-utils'
import { useFastAPI } from 'common/api'
import config from 'common/config'
import { useSnackbar } from 'notistack'

import s from './Dnd.module.scss'

export default function Dnd () {
  const theme = useTheme()
  const mainColor = theme.palette.text.secondary

  const { enqueueSnackbar } = useSnackbar()

  const dragzoneRef = useRef<HTMLDivElement>(null)

  const [fileList, setFileList] = useState<FileList>()
  const [draggingCount, setDraggingCount] = useState(0)
  const isDragging = useMemo(() => draggingCount > 0, [draggingCount])

  // Upload request
  const uploadReq = useFastAPI({ autoNotify: true, label: 'Upload' })

  // Handle upload success
  useEffect(() => {
    if (uploadReq.success) {
      setFileList(undefined)
      enqueueSnackbar('Upload successful', { variant: 'success' })
    }
  }, [enqueueSnackbar, uploadReq.success])

  // Invisible input element to trigger the 'open file' dialog
  const inputElement = useRef(document.createElement('input'))

  // Setup inputElement
  useEffect(() => {
    const input = inputElement.current
    input.type = 'file'
    input.multiple = true
    input.addEventListener('change', event => {
      const files = (event.target as any).files as FileList
      setFileList(files)
    })
    return () => {
      input.remove()
    }
  }, [])

  const openFileDialog = () => {
    inputElement.current.click()
  }

  const sendFiles = () => {
    if (fileList === undefined) throw new Error('fileList is undefined')
    const formData = new FormData()
    for (let i = 0; i < fileList.length; i++) {
      formData.append('file', fileList[i])
    }
    uploadReq.setRequest(fetch(`${config.apiBase}/api/endpoint/load`, {
      method: 'POST',
      body: formData
    }))
  }

  return (
    <div
      ref={dragzoneRef}
      className={mergeClasses(s.dnd, isDragging && s.isDragging)}
      style={{ border: `2px dashed ${isDragging ? mainColor : 'transparent'}` }}
      onClick={openFileDialog}
      onDragEnter={() => setDraggingCount(x => x + 1)}
      onDragLeave={() => setDraggingCount(x => x - 1)}
      onDragOver={event => {
        event.preventDefault()
      }}
      onDrop={event => {
        event.preventDefault()
        setDraggingCount(x => x - 1)
        const files = (event.dataTransfer || event.target).files
        setFileList(files)
      }}
    >
      <div className={s.center}>
        <CloudUploadIcon sx={{ color: mainColor, fontSize: 54 }} />
        <Typography variant='h6' component='h3' sx={{ color: mainColor }}>
          Drag and drop your files or click here
        </Typography>
        {!!fileList?.length && (
          <div className={s.buttons}>
            <Button
              variant='outlined'
              startIcon={<ClearIcon />}
              onClick={event => {
                event.stopPropagation()
                setFileList(undefined)
                uploadReq.setRequest(undefined)
              }}
            >
              Clear
            </Button>
            <LoadingButton
              variant='contained'
              startIcon={<SendIcon />}
              onClick={event => {
                event.stopPropagation()
                sendFiles()
              }}
              loading={uploadReq.loading}
            >
              Send
            </LoadingButton>
          </div>
        )}
      </div>
      <div className={s.fileList}>
        {fileList && new Array(fileList.length).fill(0).map((_, i) => (
          <Chip key={i} label={fileList.item(i)?.name} />
        ))}
      </div>
    </div>
  )
}
