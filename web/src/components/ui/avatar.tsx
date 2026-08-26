import { cn } from '@/lib/cn'

/**
 * Deterministic avatar — generates a unique gradient from the seed string.
 * Same seed always produces the same color pair.
 */

const palette = [
  ['#4338ca', '#3730a3'], // indigo
  ['#6d28d9', '#5b21b6'], // violet
  ['#be185d', '#9d174d'], // pink
  ['#be123c', '#9f1239'], // rose
  ['#b91c1c', '#991b1b'], // red
  ['#c2410c', '#9a3412'], // orange
  ['#a16207', '#854d0e'], // yellow
  ['#15803d', '#166534'], // green
  ['#0f766e', '#115e59'], // teal
  ['#0e7490', '#155e75'], // cyan
  ['#1d4ed8', '#1e40af'], // blue
  ['#7e22ce', '#6b21a8'], // purple
]

function hashCode(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0
  }
  return Math.abs(hash)
}

function getColors(seed: string): [string, string] {
  const index = hashCode(seed) % palette.length
  return palette[index] as [string, string]
}

function getInitial(name: string): string {
  return (name[0] ?? '?').toUpperCase()
}

interface AvatarProps {
  name: string
  seed?: string
  size?: 'xs' | 'sm' | 'md' | 'lg'
  className?: string
}

const sizeClasses = {
  xs: 'h-4 w-4 text-[8px] rounded',
  sm: 'h-6 w-6 text-[10px] rounded-md',
  md: 'h-7 w-7 text-[11px] rounded-md',
  lg: 'h-10 w-10 text-sm rounded-lg',
}

export function Avatar({ name, seed, size = 'md', className }: AvatarProps) {
  const [from, to] = getColors(seed ?? name)
  return (
    <div
      className={cn(
        'flex shrink-0 items-center justify-center font-bold text-white select-none',
        sizeClasses[size],
        className,
      )}
      style={{ background: `linear-gradient(135deg, ${from}, ${to})` }}
      title={name}
    >
      {getInitial(name)}
    </div>
  )
}
