// Friendly names for the generated OpenAPI schemas. Regenerate types.ts with `npm run gen:types`.
import type { components } from './types'

type S = components['schemas']

export type Flag = S['Flag']
export type FlagKind = Flag['kind']
export type Flagged = S['Flagged']
export type TeamInfo = S['TeamInfo']
export type Profile = S['Profile']
export type ProfileMap = S['ProfileMap']
export type Players = S['Players']
export type H2H = S['H2H']
export type H2HMap = S['H2HMap']
export type H2HMode = S['H2HMode']
export type Tag = H2HMap['tags'][number]
export type Scrims = S['Scrims']
export type ScrimMap = S['ScrimMap']
export type ScrimOptions = S['ScrimOptions']
export type Elo = S['Elo']
export type EloSeries = S['EloSeries']
export type Training = S['Training']
export type TrainingPlayer = S['TrainingPlayer']
export type TrainingDay = S['TrainingDay']
