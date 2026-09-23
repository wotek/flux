import { defineConfig } from 'vitepress'
import { execSync } from 'child_process'

let version = 'main'
try {
  version = execSync('git describe --tags --abbrev=0 2>/dev/null').toString().trim()
} catch (e) {
  console.log('Failed to read git tag, falling back to main')
}

export default defineConfig({
  
  title: "flux",
  description: "Event-Sourcing Framework for Go",
  themeConfig: {
    logo: '/logo.png',
    nav: [
      { text: version, link: 'https://github.com/wotek/flux/releases' },
      { text: 'Getting Started', link: '/getting-started/introduction' },
      { text: 'Tutorial', link: '/tutorial/01-project-setup' },
      { text: 'GitHub', link: 'https://github.com/wotek/flux' }
    ],
    sidebar: [
      {
        text: 'Getting Started',
        items: [
          { text: 'Introduction', link: '/getting-started/introduction' },
          { text: 'Installation', link: '/getting-started/installation' },
          { text: 'Quick Start', link: '/getting-started/quick-start' },
        ]
      },
      {
        text: 'Tutorial',
        items: [
          { text: '1. Project Setup', link: '/tutorial/01-project-setup' },
          { text: '2. Your First Aggregate', link: '/tutorial/02-first-aggregate' },
          { text: '3. Repositories & Testing', link: '/tutorial/03-repositories-and-testing' },
          { text: '4. Commands & Bus', link: '/tutorial/04-commands' },
          { text: '5. The Sales Domain', link: '/tutorial/05-sales-domain' },
          { text: '6. Building Projections', link: '/tutorial/06-projections' },
          { text: '7. Payment Workflow', link: '/tutorial/07-payment-workflow' },
        ]
      },
      {
        text: 'Guide',
        items: [
          { text: 'Events & Envelopes', link: '/guide/events' },
          { text: 'Aggregates & Changesets', link: '/guide/aggregates' },
          { text: 'Repositories & Snapshots', link: '/guide/repositories' },
          { text: 'Command, Query, and Event Buses', link: '/guide/buses' },
          { text: 'Projections', link: '/guide/projections' },
          { text: 'Workflows & Temporal Integration', link: '/guide/workflows' },
          { text: 'Structured Identifiers (URNs)', link: '/guide/identifiers' },
          { text: 'Serialization & Codecs', link: '/guide/serialization' },
        ]
      },
      {
        text: 'Backends',
        items: [
          { text: 'In-Memory (Built-in)', link: '/backends/in-memory' },
          { text: 'MySQL (Planned)', link: '/backends/mysql' },
          { text: 'Redis (Planned)', link: '/backends/redis' },
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'Architecture & Boundaries', link: '/reference/architecture' },
          { text: 'Project Layout', link: '/reference/project-layout' },
          { text: 'Core API Reference', link: '/reference/api' },
          { text: 'Testing Best Practices', link: '/reference/testing' },
        ]
      }
    ]
  }
})
