import DefaultTheme from 'vitepress/theme'

import '@fontsource/vollkorn/400.css'
import '@fontsource/vollkorn/400-italic.css'
import '@fontsource/vollkorn/500.css'
import '@fontsource/vollkorn/700.css'
import '@fontsource/fraunces/600.css'
import '@fontsource/fraunces/700.css'

import './custom.css'
import Layout from './Layout.vue'

export default {
  extends: DefaultTheme,
  Layout
}
