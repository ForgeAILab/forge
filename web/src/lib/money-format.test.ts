import { describe, expect, it } from 'vitest'

import { formatMoneyAmount, formatRateAmount } from '@/lib/money-format'
import type { MoneyAmount } from '@/types/generated/bindings/MoneyAmount'
import type { RateAmount } from '@/types/generated/bindings/RateAmount'

function money(decimal: string): MoneyAmount {
  return { currency: 'USD', decimal }
}

function rate(decimalPerMillion: string): RateAmount {
  return { currency: 'USD', decimal_per_million: decimalPerMillion }
}

describe('decimal money formatting', () => {
  it('keeps ordinary currency at two decimals and preserves explicit zero', () => {
    expect(formatMoneyAmount(money('0'))).toBe('$0.00')
    expect(formatMoneyAmount(money('1'))).toBe('$1.00')
    expect(formatMoneyAmount(money('1.2'))).toBe('$1.20')
    expect(formatMoneyAmount(money('1.234567'))).toBe('$1.23')
  })

  it('retains enough sub-cent precision for non-zero values', () => {
    expect(formatMoneyAmount(money('0.000001'))).toBe('$0.000001')
    expect(formatMoneyAmount(money('0.000000001'))).toBe('$0.000000001')
    expect(formatMoneyAmount(money('0.009999999'))).toBe('$0.009999999')
  })

  it('rounds ordinary values as decimal text without losing large integers', () => {
    expect(formatMoneyAmount(money('1.235'))).toBe('$1.24')
    expect(formatMoneyAmount(money('999999999999999999999999.999'))).toBe(
      '$1000000000000000000000000.00',
    )
  })

  it('formats rates with the per-million unit without rounding meaningful digits', () => {
    expect(formatRateAmount(rate('0'))).toBe('$0.00 / 1M tokens')
    expect(formatRateAmount(rate('1.234567'))).toBe('$1.234567 / 1M tokens')
    expect(formatRateAmount(rate('0.000001'))).toBe('$0.000001 / 1M tokens')
  })

  it('rejects signed, malformed, over-precise, and non-USD values', () => {
    for (const value of ['-0', '-1', '+1', '1e3', '1.', '.1', '1..0', 'abc', '']) {
      expect(() => formatMoneyAmount(money(value))).toThrow()
    }

    expect(() => formatMoneyAmount(money('1.1234567890'))).toThrow()
    expect(() =>
      formatMoneyAmount({ currency: 'EUR', decimal: '1' } as unknown as MoneyAmount),
    ).toThrow()
    expect(() =>
      formatRateAmount({ currency: 'EUR', decimal_per_million: '1' } as unknown as RateAmount),
    ).toThrow()
  })
})
