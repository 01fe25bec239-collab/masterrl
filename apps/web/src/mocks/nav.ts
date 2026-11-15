import type { NavEntry } from '../app/nav.registry';

/**
 * Dev-only pages that show every UI kit part working against mock data.
 * Added to the nav by main.tsx when the mock API is on; never in production builds.
 */
export const mockNav: readonly NavEntry[] = [
  {
    id: 'demo-components',
    label: 'Components (demo)',
    path: '/demo/components',
    icon: 'grid',
    section: 'project',
    keywords: ['table', 'chip', 'dialog', 'wizard', 'upload'],
    load: () => import('./gallery/ComponentsGallery'),
    homeCard: () => import('./gallery/HomeCard'),
  },
  {
    id: 'demo-charts',
    label: 'Charts (demo)',
    path: '/demo/charts',
    icon: 'chart',
    section: 'project',
    load: () => import('./gallery/ChartsGallery'),
  },
  {
    id: 'demo-forms',
    label: 'Forms (demo)',
    path: '/demo/forms',
    icon: 'file',
    section: 'project',
    keywords: ['schema', 'yaml', 'preset'],
    load: () => import('./gallery/FormsGallery'),
  },
  {
    id: 'demo-live',
    label: 'Live data (demo)',
    path: '/demo/live',
    icon: 'radio',
    section: 'project',
    level: 'advanced',
    keywords: ['stream', 'logs', 'metrics'],
    load: () => import('./gallery/LiveGallery'),
  },
];
