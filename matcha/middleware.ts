import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { getSessionFromToken, COOKIE_NAME } from "@/lib/auth";

const protectedPaths = [
  "/dashboard",
  "/profile",
  "/matches",
  "/messages",
  "/settings",
  "/admin",
];

const authPaths = ["/login", "/register"];

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const token = request.cookies.get(COOKIE_NAME)?.value;
  const session = await getSessionFromToken(token);

  const isProtected = protectedPaths.some(
    (path) => pathname === path || pathname.startsWith(`${path}/`)
  );
  const isAuthPage = authPaths.some((path) => pathname === path);
  const isAdmin = pathname === "/admin" || pathname.startsWith("/admin/");

  if (isProtected && !session) {
    const loginUrl = new URL("/login", request.url);
    loginUrl.searchParams.set("callbackUrl", pathname);
    return NextResponse.redirect(loginUrl);
  }

  if (isAdmin && session && session.role !== "ADMIN") {
    return NextResponse.redirect(new URL("/dashboard", request.url));
  }

  if (isAuthPage && session) {
    return NextResponse.redirect(new URL("/dashboard", request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    "/dashboard/:path*",
    "/profile/:path*",
    "/matches/:path*",
    "/messages/:path*",
    "/settings/:path*",
    "/admin/:path*",
    "/login",
    "/register",
  ],
};
