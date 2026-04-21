import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Servus.Akka',
  description: 'Akka.NET quality-of-life extension library',
  lang: 'en-US',
  cleanUrls: true,
  srcExclude: ['superpowers/**'],
  head: [
    ['link', { rel: 'icon', type: 'image/png', href: '/logo.png' }]
  ],
  themeConfig: {
    logo: '/logo.png',
    siteTitle: 'Servus.Akka',
    search: {
      provider: 'local'
    },
    socialLinks: [
      { icon: 'github', link: 'https://github.com/Bavaria-Black/servus.akka' }
    ],
    nav: [
      { text: 'Get Started', link: '/getting-started' },
      {
        text: 'Modules',
        items: [
          {
            text: 'Core',
            items: [
              { text: 'Extensions', link: '/modules/extensions/' },
              { text: 'Dependency Injection', link: '/modules/dependency-injection/' },
              { text: 'Startup', link: '/modules/startup/' }
            ]
          },
          {
            text: 'Messaging',
            items: [
              { text: 'Diagnostics', link: '/modules/diagnostics/' },
              { text: 'Messaging', link: '/modules/messaging/' }
            ]
          }
        ]
      },
      {
        text: 'Resources',
        items: [
          { text: 'GitHub Repository', link: 'https://github.com/Bavaria-Black/servus.akka' },
          { text: 'NuGet Package', link: 'https://www.nuget.org/packages/Servus.Akka/' },
          { text: 'Report an Issue', link: 'https://github.com/Bavaria-Black/servus.akka/issues' }
        ]
      }
    ],
    sidebar: {
      '/': [
        {
          text: 'Introduction',
          collapsed: false,
          items: [
            { text: 'Getting Started', link: '/getting-started' }
          ]
        },
        {
          text: 'Extensions',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/modules/extensions/' },
            { text: 'Register Extensions', link: '/modules/extensions/register' },
            { text: 'Resolve Extensions', link: '/modules/extensions/resolve' },
            { text: 'Registry Extensions', link: '/modules/extensions/registry' },
            { text: 'Context Extensions', link: '/modules/extensions/context' },
            { text: 'Akka Option Match', link: '/modules/extensions/akka-options' }
          ]
        },
        {
          text: 'Dependency Injection',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/modules/dependency-injection/' },
            { text: 'ActorRef<TActor>', link: '/modules/dependency-injection/actor-ref' },
            { text: 'ActorRefProviderFactory', link: '/modules/dependency-injection/actor-ref-provider-factory' },
            { text: 'ActorRefServiceProvider', link: '/modules/dependency-injection/actor-ref-service-provider' }
          ]
        },
        {
          text: 'Diagnostics',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/modules/diagnostics/' },
            { text: 'Traced Message Extensions', link: '/modules/diagnostics/traced-message-extensions' },
            { text: 'Traced Message Actor', link: '/modules/diagnostics/traced-message-actor' }
          ]
        },
        {
          text: 'Messaging',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/modules/messaging/' },
            { text: 'Traced Message Envelope', link: '/modules/messaging/traced-message-envelope' }
          ]
        },
        {
          text: 'Startup',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/modules/startup/' },
            { text: 'ActorSystemSetupContainer', link: '/modules/startup/actor-system-setup-container' },
            { text: 'ActorRefProviderStartupContainer', link: '/modules/startup/actor-ref-provider-startup-container' }
          ]
        }
      ]
    },
    footer: {
      message: 'Pfiat di und happy coding! 🥨🍺',
      copyright: '© 2026 Leberkas.org · MIT License'
    },
    notFound: {
      code: '404',
      title: 'Ois is weg!',
      quote: 'De Seitn is ned gfundn worn. Vielleicht host di im Actor-System verlaufn?',
      linkLabel: 'Zruck zur Startseitn',
      linkText: 'Zruck zur Startseitn'
    }
  }
})
