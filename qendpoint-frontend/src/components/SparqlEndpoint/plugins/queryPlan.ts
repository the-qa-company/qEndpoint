import Yasr from '@triply/yasr'
import * as faFlask from '@fortawesome/free-solid-svg-icons/faFlask'

export interface PluginConfig {
}

const buildPlan = (plan: any) => {
  const container = document.createElement('li')
  const id = document.createElement('p')

  if (plan.error) {
    id.innerText = plan.error
  } else if (plan.plans) {
    id.innerText = plan.id
    const ul = document.createElement('ul')
    ul.style.listStyleType = 'circle'
    plan.plans.forEach((pln: any) => {
      ul.appendChild(buildPlan(pln))
    })
    container.appendChild(ul)
  }
  container.appendChild(id)
  return container
}

export default class QueryPlanPlugin {
  // A priority value. If multiple plugin support rendering of a result, this value is used
  // to select the correct plugin
  priority = 10

  // Whether to show a select-button for this plugin
  yasr: Yasr

  label = 'Query plan'

  constructor (yasr: Yasr) {
    this.yasr = yasr
  }

  // Draw the resultset. This plugin simply draws the string 'True' or 'False'
  draw () {
    const json = this.yasr.results?.getAsJson() as any
    if (!json || !json.plan) {
      const el = document.createElement('div')
      el.innerHTML = 'No query plan found, please add <strong>#get_plan</strong> in you query to fetch it.'
      this.yasr.resultsEl.appendChild(el)
    } else {
      const plan = json.plan
      const plnEl = buildPlan(plan)
      console.log(plnEl)
      const container = document.createElement('pre')
      container.innerText = JSON.stringify(plan, null, 2)
      this.yasr.resultsEl.appendChild(container)
    }
  }

  // A required function, used to indicate whether this plugin can draw the current
  // resultset from yasr
  canHandleResults () {
    return true
  }

  // A required function, used to identify the plugin, works best with an svg
  getIcon () {
    const svgString = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${faFlask.width} ${faFlask.height}" aria-hidden="true"><path fill="currentColor" d="${faFlask.svgPathData}"></path></svg>`
    const template = document.createElement('template')
    template.innerHTML = svgString
    return template.content.firstChild
  }
}
