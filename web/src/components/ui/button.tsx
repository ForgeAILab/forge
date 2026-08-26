import { forwardRef, type ButtonHTMLAttributes } from 'react'
import { buttonClassName, type ButtonSize, type ButtonVariant } from '@/components/ui/button-styles'

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'default', size = 'default', type = 'button', ...props }, ref) => {
    return (
      <button
        ref={ref}
        type={type}
        className={buttonClassName({ variant, size, className })}
        {...props}
      />
    )
  },
)
Button.displayName = 'Button'
