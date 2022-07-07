import config from './config.json'

const dev = process.env.REACT_APP_DEV === 'true'

let apiBase: String

if (dev) {
  apiBase = config.apiBaseDev
} else {
  apiBase = window.location.origin
}

export default {
  ...config,
  dev,
  apiBase
}
