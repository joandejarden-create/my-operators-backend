import { z } from "zod";

export const profileSchema = z.object({
  displayName: z
    .string()
    .min(2, "Display name must be at least 2 characters")
    .max(50),
  bio: z.string().max(500).optional().nullable(),
  age: z
    .number()
    .int()
    .min(18, "Must be at least 18")
    .max(120)
    .optional()
    .nullable(),
  location: z.string().max(100).optional().nullable(),
  interests: z.array(z.string().max(30)).max(20).optional(),
  photoUrl: z.string().url().optional().nullable().or(z.literal("")),
});

export type ProfileInput = z.infer<typeof profileSchema>;
