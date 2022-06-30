import React, { ChangeEventHandler, useCallback, useEffect, useState } from 'react'
import { Box, Button, Modal, Typography, useTheme } from '@mui/material'
import ClearIcon from '@mui/icons-material/Clear'
import EditIcon from '@mui/icons-material/Edit'
import AddIcon from '@mui/icons-material/Add'
import SaveIcon from '@mui/icons-material/Save'
import { useFastAPI } from 'common/api'
import config from 'common/config'

import s from './index.module.scss'
import useAsyncEffect from 'use-async-effect'

interface NamespaceDataI {
  prefix: string
  name: string
}

export default function Prefixes () {
  const theme = useTheme()

  const [modalOpen, setModalOpen] = useState<boolean>(false)
  const [prefix, setPrefix] = useState<string>('')
  const [editPrefix, setEditPrefix] = useState<string>('')
  const [prefixName, setPrefixName] = useState<string>('')
  const [updated, setUpdates] = useState<boolean>(false)
  const [loaded, setLoaded] = useState<boolean>(false)
  const [prefixes, setPrefixes] = useState<Record<string, string>>({})

  // prefixes request
  const prefixesReq = useFastAPI({ autoNotify: true, errorMsg: 'Impossible to get the prefixes' })
  const {
    setRequest: setPrefixesRequest
  } = prefixesReq

  const checkPrefixes = useCallback(() => {
    setPrefixesRequest(fetch(`${config.apiBase}/endpoint/prefixes`))
  }, [setPrefixesRequest])

  // Make prefixes request on mount
  useEffect(() => {
    checkPrefixes()
  }, [checkPrefixes])

  // Read prefixes response
  useAsyncEffect(async () => {
    if (!loaded && prefixesReq.success && prefixesReq.res !== undefined) {
      const parsed = await prefixesReq.res.json()
      setPrefixes({ ...parsed })
      setLoaded(true)
    }
  }, [prefixesReq.success, prefixesReq.res])

  // prefixes request
  const prefixesSetReq = useFastAPI({ autoNotify: true, errorMsg: 'Impossible to save the prefixes' })
  const {
    setRequest: setPrefixesSetRequest
  } = prefixesSetReq

  const sendPrefixes = useCallback(() => {
    setPrefixesSetRequest(fetch(`${config.apiBase}/endpoint/setprefixes`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(prefixes)
    }))
  }, [setPrefixesSetRequest, prefixes])

  // Read set prefixes response (set the prefixes)
  useAsyncEffect(async () => {
    if (prefixesSetReq.success && prefixesSetReq.res !== undefined) {
      const parsed = await prefixesSetReq.res.json()
      setPrefixes({ ...parsed })
      setLoaded(true)
    }
  }, [prefixesSetReq.success, prefixesSetReq.res])

  const setPrefixValue: ChangeEventHandler<HTMLInputElement> = (e) => {
    setPrefix(e.target.value)
  }
  const setPrefixNameValue: ChangeEventHandler<HTMLInputElement> = (e) => {
    setPrefixName(e.target.value)
  }

  const closeModal = () => {
    setPrefix('')
    setPrefixName('')
    setEditPrefix('')
    setModalOpen(false)
  }
  const openModal = () => {
    setModalOpen(true)
  }

  /**
   * add the current prefix to the prefixes and reset the inputs
   */
  const add = () => {
    const nprefixes = {
      ...prefixes
    }
    nprefixes[prefix] = prefixName
    if (editPrefix !== '' && editPrefix !== prefix) {
      delete nprefixes[editPrefix]
    }
    setPrefixes(nprefixes)
    setUpdates(true)
    closeModal()
  }

  /**
   * create a function to remove a prefix
   * @param prefix the prefix to remove
   * @returns {() => void} a function to remove this prefix
   */
  const remove = (prefix: NamespaceDataI): () => void => () => {
    const nprefixes = {
      ...prefixes
    }
    delete nprefixes[prefix.prefix]
    setPrefixes(nprefixes)
    setUpdates(true)
  }
  /**
   * set this prefix into the inputs fields
   * @param prefix the prefix
   * @returns {() => void} a function to edit this prefix
   */
  const edit = (prefix: NamespaceDataI): () => void => () => {
    setPrefix(prefix.prefix)
    setEditPrefix(prefix.prefix)
    setPrefixName(prefix.name)
    openModal()
  }

  /**
   * @returns {boolean} allow add of the input prefix
   */
  const allowAdd = (): boolean => {
    return prefix.length !== 0 && prefixName.length !== 0
  }

  const savePrefix = async () => {
    sendPrefixes()
    setUpdates(false)
  }

  const isNotEdit = () => !(prefixes[prefix] || editPrefix !== '')
  return (
    <div className={s.container}>
      <div className={s.centered}>
        <Typography className={s.centered} sx={{ color: theme.palette.text.secondary, marginTop: '30px' }} variant='h4'>Prefixes</Typography>
        <div style={{ color: theme.palette.text.secondary }}>
          Set the default prefixes for the queries on the endpoint.
        </div>
      </div>
      <div className={s.body}>
        {!loaded && (<div>Waiting for the endpoint response...</div>)}
        {loaded && (
          <div>
            <Modal
              open={modalOpen}
              onClose={closeModal}
              aria-labelledby='modal-modal-title'
              aria-describedby='modal-modal-description'
            >
              <Box
                className={s.modalBox}
              >
                <div
                  className={s.centered}
                >
                  <div>Prefix</div>
                  <div><input type='text' value={prefix} onChange={setPrefixValue} /></div>
                  <div>IRI</div>
                  <div><input type='text' value={prefixName} onChange={setPrefixNameValue} /></div>
                  {!(prefixes[prefix] && (editPrefix === '' || editPrefix !== prefix)) || (<p>This prefix already exists, changes will overwrite the previous value.</p>)}
                  <p>
                    <Button
                      variant='outlined'
                      startIcon={(isNotEdit()) ? <AddIcon /> : <EditIcon />}
                      onClick={add}
                      disabled={!allowAdd()}
                    >
                      {(isNotEdit()) ? 'Add' : 'Edit'}
                    </Button>
                    <Button
                      variant='outlined'
                      startIcon={<ClearIcon />}
                      onClick={closeModal}
                    >
                      Cancel
                    </Button>
                  </p>
                </div>
              </Box>
            </Modal>
            <div className={s.centered}>
              <Button
                variant='contained'
                startIcon={<AddIcon />}
                onClick={openModal}
              >
                New
              </Button>

              <Button
                variant='contained'
                startIcon={<SaveIcon />}
                onClick={savePrefix}
                disabled={!updated}
              >
                Save
              </Button>
            </div>
            <div className={s.prefixes}>
              {Object.entries(prefixes).sort(([prefix1, name1], [prefix2, name2]) => {
                if (prefix1 < prefix2) {
                  return -1
                }
                if (prefix1 === prefix2) {
                  return 0
                }
                return 1
              }).map(
                ([prefix, name]) => {
                  return (
                    <div key={prefix} className={s.prefix}>
                      <p>
                        PREFIX&nbsp;
                        <span className={s.prefix_prefix}>{prefix}:</span>&nbsp;
                        <span className={s.prefix_name}>&lt;{name}&gt;</span>
                      </p>
                      <div>
                        <Button
                          variant='outlined'
                          startIcon={<EditIcon />}
                          onClick={edit({
                            prefix,
                            name
                          })}
                        >
                          Edit
                        </Button>
                        <Button
                          variant='outlined'
                          startIcon={<ClearIcon />}
                          onClick={remove({
                            prefix,
                            name
                          })}
                        >
                          Remove
                        </Button>
                      </div>
                    </div>
                  )
                }
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
