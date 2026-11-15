import { useId, useState, type ComponentType, type ReactNode } from 'react';
import { cn } from '../lib/cn';
import { Button, focusRing } from './Button';
import { Icon, type IconName } from './Icon';

export interface LinkProps {
  href: string;
  className?: string;
  'aria-current'?: 'page';
  children: ReactNode;
}

/** Plain anchor; apps pass their router's link through `linkComponent`. */
export function PlainLink({ href, className, children, ...rest }: LinkProps) {
  return (
    <a href={href} className={className} {...rest}>
      {children}
    </a>
  );
}

export interface AppShellProps {
  /** Product name shown top left; links to `homeHref`. */
  brand: string;
  homeHref?: string;
  /** Scope switcher, search, toggles… */
  topBar?: ReactNode;
  /** Usually a <LeftNav>. */
  nav?: ReactNode;
  /** Shown above the page, e.g. a "live updates paused" banner. */
  banner?: ReactNode;
  linkComponent?: ComponentType<LinkProps>;
  children: ReactNode;
}

/** Page frame: skip link, top bar, collapsible left nav, main area. */
export function AppShell({
  brand,
  homeHref = '/',
  topBar,
  nav,
  banner,
  linkComponent: Link = PlainLink,
  children,
}: AppShellProps) {
  const [navOpen, setNavOpen] = useState(false);
  const navId = useId();
  return (
    <div className="flex min-h-screen flex-col bg-bg text-fg">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:left-2 focus:top-2 focus:z-50 focus:rounded-md focus:bg-accent focus:px-3 focus:py-2 focus:text-accent-fg"
      >
        Skip to main content
      </a>
      <header className="flex flex-wrap items-center gap-3 border-b border-border bg-surface px-4 py-2">
        {nav && (
          <Button
            variant="ghost"
            size="sm"
            className="lg:hidden"
            aria-expanded={navOpen}
            aria-controls={navId}
            onClick={() => {
              setNavOpen(!navOpen);
            }}
          >
            <Icon name="list" />
            Menu
          </Button>
        )}
        <Link href={homeHref} className={cn('font-semibold text-fg', focusRing)}>
          {brand}
        </Link>
        <div className="flex flex-1 flex-wrap items-center justify-end gap-3">{topBar}</div>
      </header>
      <div className="flex flex-1">
        {nav && (
          <div
            id={navId}
            className={cn(
              'w-60 shrink-0 border-r border-border bg-bg',
              navOpen ? 'block' : 'hidden lg:block',
            )}
          >
            {nav}
          </div>
        )}
        <main id="main" tabIndex={-1} className="min-w-0 flex-1 p-6 outline-none">
          {banner}
          {children}
        </main>
      </div>
    </div>
  );
}

export interface NavItem {
  id: string;
  label: string;
  href: string;
  icon?: IconName;
}

export interface NavSection {
  id: string;
  /** Section heading; omit for the first, unnamed section. */
  label?: string;
  items: NavItem[];
}

export interface LeftNavProps {
  sections: NavSection[];
  /** Current path; the matching item gets `aria-current="page"`. */
  currentPath: string;
  linkComponent?: ComponentType<LinkProps>;
  label?: string;
}

const isActive = (href: string, path: string) =>
  href === '/' ? path === '/' : path === href || path.startsWith(`${href}/`);

export function LeftNav({
  sections,
  currentPath,
  linkComponent: Link = PlainLink,
  label = 'Main',
}: LeftNavProps) {
  return (
    <nav aria-label={label} className="flex flex-col gap-4 p-3">
      {sections
        .filter((s) => s.items.length > 0)
        .map((section) => (
          <div key={section.id}>
            {section.label && (
              <p className="px-2 pb-1 text-xs font-semibold uppercase tracking-wide text-fg-muted">
                {section.label}
              </p>
            )}
            <ul className="flex flex-col gap-0.5">
              {section.items.map((item) => {
                const active = isActive(item.href, currentPath);
                return (
                  <li key={item.id}>
                    <Link
                      href={item.href}
                      aria-current={active ? 'page' : undefined}
                      className={cn(
                        'flex items-center gap-2 rounded-md px-2 py-1.5 text-sm',
                        active ? 'bg-accent text-accent-fg' : 'text-fg hover:bg-surface',
                        focusRing,
                      )}
                    >
                      {item.icon && <Icon name={item.icon} />}
                      {item.label}
                    </Link>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
    </nav>
  );
}

export interface ScopeOption {
  id: string;
  name: string;
}

export interface ScopeLevel {
  /** e.g. `org`, `workspace`, `project`. */
  id: string;
  /** Visible label, e.g. "Organization". */
  label: string;
  options: ScopeOption[];
  value: string | undefined;
  onChange: (id: string) => void;
}

/** Organization → Workspace → Project pickers. Levels with one option still show, read-only. */
export function ScopeSwitcher({ levels }: { levels: ScopeLevel[] }) {
  const baseId = useId();
  return (
    <div
      role="group"
      aria-label="Where you are working"
      className="flex flex-wrap items-center gap-2"
    >
      {levels.map((level, i) => {
        const id = `${baseId}-${level.id}`;
        return (
          <div key={level.id} className="flex items-center gap-1">
            {i > 0 && <Icon name="chevronRight" className="text-fg-muted" />}
            <label htmlFor={id} className="sr-only">
              {level.label}
            </label>
            <select
              id={id}
              className={cn(
                'max-w-48 rounded-md border border-border bg-bg px-2 py-1 text-sm text-fg',
                focusRing,
              )}
              value={level.value ?? ''}
              disabled={level.options.length <= 1}
              onChange={(e) => {
                level.onChange(e.target.value);
              }}
            >
              {level.value === undefined && (
                <option value="">Choose {level.label.toLowerCase()}</option>
              )}
              {level.options.map((o) => (
                <option key={o.id} value={o.id}>
                  {o.name}
                </option>
              ))}
            </select>
          </div>
        );
      })}
    </div>
  );
}

export interface PageHeaderProps {
  title: string;
  description?: ReactNode;
  /** Buttons on the right. */
  actions?: ReactNode;
  /** Badges next to the title, e.g. a StateChip. */
  meta?: ReactNode;
  className?: string;
}

export function PageHeader({ title, description, actions, meta, className }: PageHeaderProps) {
  return (
    <div className={cn('mb-6 flex flex-wrap items-start gap-4', className)}>
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-semibold text-fg">{title}</h1>
          {meta}
        </div>
        {description && <p className="mt-1 max-w-prose text-fg-muted">{description}</p>}
      </div>
      {actions && <div className="flex flex-wrap gap-2">{actions}</div>}
    </div>
  );
}
