using Ecommerce.AppService.Services;

var builder = WebApplication.CreateBuilder(args);

// Register ASP.NET Core MVC (Controllers + Razor Views) and AgriStream IoT Cloud Service
builder.Services.AddControllersWithViews();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSingleton<AgriStreamDataService>();

var app = builder.Build();

app.UseStaticFiles();
app.UseRouting();

app.MapControllers();
app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
