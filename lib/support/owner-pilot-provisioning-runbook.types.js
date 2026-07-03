/**
 * @typedef {{ label: string, variant?: 'internal' }} RunbookBadge
 * @typedef {{ type: string, html?: string, text?: string, level?: number, headers?: string[], rows?: string[][], items?: string[], className?: string, variant?: string, title?: string }} RunbookContentBlock
 * @typedef {{ id: string, title: string, defaultOpen?: boolean, contentBlocks: RunbookContentBlock[] }} RunbookSection
 * @typedef {{ title: string, subtitle?: string, badges?: RunbookBadge[], warning?: string, sections: RunbookSection[] }} OwnerPilotRunbook
 */

export {};
