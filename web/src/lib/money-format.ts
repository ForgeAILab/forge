import type { MoneyAmount } from '@/types/generated/bindings/MoneyAmount'
import type { RateAmount } from '@/types/generated/bindings/RateAmount'

const MAX_FRACTION_DIGITS = 9

type DecimalParts = {
  integer: string
  fraction: string
}

const NEXT_DIGIT: Record<string, string> = {
  '0': '1',
  '1': '2',
  '2': '3',
  '3': '4',
  '4': '5',
  '5': '6',
  '6': '7',
  '7': '8',
  '8': '9',
}

function decimalParts(value: string, fieldName: string): DecimalParts {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${fieldName} must be a decimal string`)
  }

  const match = /^(\d+)(?:\.(\d+))?$/.exec(value)
  if (!match) {
    throw new Error(`${fieldName} must be a non-negative decimal without an exponent`)
  }

  const fraction = match[2] ?? ''
  if (fraction.length > MAX_FRACTION_DIGITS) {
    throw new Error(`${fieldName} has too many fractional digits`)
  }

  return {
    integer: match[1].replace(/^0+(?=\d)/, ''),
    fraction: fraction.replace(/0+$/, ''),
  }
}

function incrementDigits(value: string): string {
  const digits = value.split('')
  let carry = true

  for (let index = digits.length - 1; index >= 0 && carry; index -= 1) {
    const digit = digits[index]
    if (digit === '9') {
      digits[index] = '0'
      continue
    }

    const nextDigit = NEXT_DIGIT[digit]
    if (!nextDigit) {
      throw new Error('Cannot round malformed decimal digits')
    }
    digits[index] = nextDigit
    carry = false
  }

  if (carry) digits.unshift('1')
  return digits.join('')
}

function isAtLeastOneCent(parts: DecimalParts): boolean {
  if (parts.integer !== '0') return true
  return parts.fraction.padEnd(2, '0').slice(0, 2) >= '01'
}

function displayDecimal(parts: DecimalParts): string {
  if (parts.integer === '0' && parts.fraction.length === 0) return '0.00'

  // Keep the ordinary currency display at cents, while retaining every
  // meaningful sub-cent digit for tiny values. This ensures a positive
  // amount can never disappear into "$0.00".
  if (!isAtLeastOneCent(parts)) {
    return `0.${parts.fraction}`
  }

  const cents = parts.fraction.padEnd(2, '0').slice(0, 2)
  const rounded = parts.fraction.length > 2 && parts.fraction[2] >= '5'
  const roundedDigits = rounded
    ? incrementDigits(`${parts.integer}${cents}`)
    : `${parts.integer}${cents}`

  return `${roundedDigits.slice(0, -2)}.${roundedDigits.slice(-2)}`
}

function displayRateDecimal(parts: DecimalParts): string {
  if (parts.fraction.length === 0) return `${parts.integer}.00`
  return `${parts.integer}.${parts.fraction}`
}

function assertUsd(currency: string, fieldName: string): void {
  if (currency !== 'USD') {
    throw new Error(`${fieldName}.currency must be USD`)
  }
}

/** Format a canonical API money amount without floating-point conversion. */
export function formatMoneyAmount(amount: MoneyAmount): string {
  assertUsd(amount.currency, 'MoneyAmount')
  return `$${displayDecimal(decimalParts(amount.decimal, 'MoneyAmount.decimal'))}`
}

/** Format a canonical per-million-token rate with its required unit. */
export function formatRateAmount(rate: RateAmount): string {
  assertUsd(rate.currency, 'RateAmount')
  return `$${displayRateDecimal(decimalParts(rate.decimal_per_million, 'RateAmount.decimal_per_million'))} / 1M tokens`
}
