import DefaultTheme from 'vitepress/theme'

import '@fontsource/inter/400.css'
import '@fontsource/inter/500.css'
import '@fontsource/inter/600.css'
import '@fontsource/fraunces/600.css'
import '@fontsource/fraunces/700.css'

import './custom.css'
import Layout from './Layout.vue'

export default {
  extends: DefaultTheme,
  Layout
}
