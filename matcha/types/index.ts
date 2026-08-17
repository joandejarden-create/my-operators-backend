import type { Role } from "@prisma/client";

export interface SessionUser {
  id: string;
  email: string;
  role: Role;
}

export interface ApiResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

export interface ProfileResponse {
  id: string;
  userId: string;
  displayName: string;
  bio: string | null;
  age: number | null;
  location: string | null;
  interests: string[];
  photoUrl: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface MatchResponse {
  id: string;
  score: number | null;
  status: string;
  createdAt: string;
  profile: ProfileResponse;
}

export interface MessageResponse {
  id: string;
  content: string;
  read: boolean;
  createdAt: string;
  sender: {
    id: string;
    displayName: string;
    photoUrl: string | null;
  };
  receiver: {
    id: string;
    displayName: string;
    photoUrl: string | null;
  };
}

export interface AdminUserResponse {
  id: string;
  email: string;
  role: Role;
  createdAt: string;
  profile: ProfileResponse | null;
}

export interface AdminStatsResponse {
  totalUsers: number;
  totalMatches: number;
  totalMessages: number;
  usersWithProfiles: number;
}
