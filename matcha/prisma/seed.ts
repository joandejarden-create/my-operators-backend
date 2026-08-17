import { PrismaClient, Role } from "@prisma/client";
import bcrypt from "bcryptjs";

const prisma = new PrismaClient();

async function main() {
  const adminPassword = await bcrypt.hash("admin123", 10);
  const userPassword = await bcrypt.hash("password123", 10);

  const admin = await prisma.user.upsert({
    where: { email: "admin@matcha.com" },
    update: {},
    create: {
      email: "admin@matcha.com",
      password: adminPassword,
      role: Role.ADMIN,
      profile: {
        create: {
          displayName: "Admin",
          bio: "Matcha platform administrator",
          age: 30,
          location: "San Francisco, CA",
          interests: "coffee,matcha,tech",
        },
      },
    },
  });

  const demoUsers = [
    {
      email: "alice@matcha.com",
      displayName: "Alice Chen",
      bio: "Love hiking and specialty coffee.",
      age: 28,
      location: "Portland, OR",
      interests: "hiking,coffee,photography",
    },
    {
      email: "bob@matcha.com",
      displayName: "Bob Martinez",
      bio: "Foodie and weekend chef.",
      age: 32,
      location: "Austin, TX",
      interests: "cooking,music,travel",
    },
    {
      email: "carol@matcha.com",
      displayName: "Carol Williams",
      bio: "Yoga instructor and book lover.",
      age: 26,
      location: "Denver, CO",
      interests: "yoga,reading,meditation",
    },
  ];

  for (const user of demoUsers) {
    await prisma.user.upsert({
      where: { email: user.email },
      update: {},
      create: {
        email: user.email,
        password: userPassword,
        role: Role.USER,
        profile: {
          create: {
            displayName: user.displayName,
            bio: user.bio,
            age: user.age,
            location: user.location,
            interests: user.interests,
          },
        },
      },
    });
  }

  console.log("Seed complete:", { admin: admin.email, demoUsers: demoUsers.length });
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
