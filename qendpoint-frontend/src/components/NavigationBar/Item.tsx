import { ListItemButton, ListItemIcon, ListItemText, Tooltip } from '@mui/material'
import React, { useEffect } from 'react'

interface Props {
  icon: React.ReactNode;
  name: string;
  open: boolean;
  onClick?: () => void;
  selected?: boolean;
  showName?: boolean;
}

export default function Item (props: Props) {
  const { icon, name, open, onClick, selected, showName } = props
  const [tooltipOpen, setTooltipOpen] = React.useState(false)

  useEffect(() => {
    if (open) setTooltipOpen(false)
  }, [open])

  return (
    <Tooltip
      title={name}
      placement='right'
      open={tooltipOpen}
      onOpen={() => !open && setTooltipOpen(true)}
      onClose={() => setTooltipOpen(false)}
    >
      <ListItemButton
        onClick={onClick}
        selected={selected}
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '4px 0 4px 0'
        }}
      >
        <ListItemIcon
          sx={{
            minWidth: 0,
            width: 50,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center'
          }}
        >
          {icon}
        </ListItemIcon>
        {showName && <ListItemText sx={{ whiteSpace: 'nowrap' }} primary={name} />}
      </ListItemButton>
    </Tooltip>
  )
}
