# Dealality Master To-Do — view setup (manual)

Base: `appKZuK006BWIVjNW`
Table: **Founder Project Plan** (`tblpCg0QZ0kIPXihE`)

Filter master tasks with `{Source} = 'ChatGPT Master To-Do'` once that field exists.

## Master To-Do — Active

```
AND({Status} != 'Completed', {Status} != 'Deferred', {Status} != 'Not Needed')
```

Note: Also hide legacy founder-plan rows by adding Source = ChatGPT Master To-Do when that field exists.

## P1 Today / This Week

```
AND({Priority} = 'P1 = Important Near-Term', {Status} != 'Completed')
```

Sort: Priority asc, End asc

## GTM / Outreach

```
OR({Phase} = 'GTM / Outreach', {Workstream} = 'Outreach Execution', {Workstream} = 'Pilot Target List', {Workstream} = 'Reply Handling')
```

## Pilot Delivery

```
{Phase} = 'Pilot Delivery'
```

## Access Hygiene

```
{Workstream} = 'Access Hygiene'
```

## Completed

```
{Status} = 'Completed'
```

## Deferred / Later

```
OR({Status} = 'Deferred', {Phase} = 'Later')
```

