# syntax=docker/dockerfile:1

# ---- Build stage: restore and publish a self-contained framework-dependent app ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY RabbitMqFundamentals.csproj ./
RUN dotnet restore
COPY . ./
RUN dotnet publish -c Release -o /app

# ---- Runtime stage: ASP.NET Core runtime + published output ----
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app ./
CMD ["dotnet", "RabbitMqFundamentals.dll"]
