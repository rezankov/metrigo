import "./globals.css";

export const metadata = {
  title: "Metrigo",
  description: "AI-first analytics for Wildberries sellers",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ru">
      <body>{children}</body>
    </html>
  );
}