const prefix = 'hdtqs-'

// Define local storage interface
export interface LocalStorageI {
  isNavigationBarOpen: boolean
}

const getItemName = (name: string) => prefix + name

export const setItem = <T extends keyof LocalStorageI>(key: T, value: LocalStorageI[T] | undefined) => {
  if (value === undefined) {
    localStorage.removeItem(getItemName(key))
  } else {
    localStorage.setItem(getItemName(key), JSON.stringify(value))
  }
}

export const getItem = <T extends keyof LocalStorageI>(key: T): LocalStorageI[T] | undefined => {
  const item = localStorage.getItem(getItemName(key))
  if (item === null) {
    return undefined
  }
  try {
    return JSON.parse(item)
  } catch (e) {
    console.error(e)
    return undefined
  }
}
