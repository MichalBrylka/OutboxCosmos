using System.Reflection;

namespace PolymorphSerdes.Tests;

public static class MessageFactory
{
    public static object CreateRandomMessage(Random random, Type type)
    {
        var ctor = type
            .GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length)
            .FirstOrDefault();

        if (ctor != null)
        {
            var args = ctor.GetParameters()
                .Select(p => GenerateValue(random, p.ParameterType))
                .ToArray();

            return ctor.Invoke(args);
        }

        // fallback for non-record / POCO types
        var instance = Activator.CreateInstance(type)
            ?? throw new InvalidOperationException($"Cannot create {type.Name}");

        PopulateProperties(random, instance);
        return instance;
    }

    // -----------------------------
    // property-based population
    // -----------------------------
    private static void PopulateProperties(Random random, object instance)
    {
        var type = instance.GetType();

        foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!prop.CanWrite) continue;

            prop.SetValue(instance, GenerateValue(random, prop.PropertyType));
        }
    }

    // -----------------------------
    // value generator
    // -----------------------------
    private static object? GenerateValue(Random random, Type type)
    {
        // strings
        if (type == typeof(string))
            return RandomString(random);

        // char
        if (type == typeof(char))
            return (char)random.Next(65, 90); // A-Z

        // integers (all widths)
        if (type == typeof(byte))
            return (byte)random.Next(byte.MinValue, byte.MaxValue);

        if (type == typeof(short))
            return (short)random.Next(short.MinValue, short.MaxValue);

        if (type == typeof(int))
            return random.Next();

        if (type == typeof(long))
            return random.NextInt64();

        // floating point numbers
        if (type == typeof(float))
            return (float)random.NextDouble() * random.Next(-1000, 1000);

        if (type == typeof(double))
            return random.NextDouble() * random.Next(-1000, 1000);

        if (type == typeof(decimal))
            return (decimal)random.NextDouble() * random.Next(-1000, 1000);

        // boolean
        if (type == typeof(bool))
            return random.Next(0, 2) == 0;

        // DateTime types
        if (type == typeof(DateTime))
            return DateTime.UtcNow.AddTicks(random.Next(-100000, 100000));

        if (type == typeof(DateTimeOffset))
            return new DateTimeOffset(DateTime.UtcNow.AddTicks(random.Next(-100000, 100000)));

        if (type == typeof(DateOnly))
            return DateOnly.FromDateTime(DateTime.UtcNow.AddDays(random.Next(-1000, 1000)));

        if (type == typeof(TimeOnly))
            return TimeOnly.FromTimeSpan(TimeSpan.FromSeconds(random.Next(0, 86400)));

        // enums
        if (type.IsEnum)
        {
            var values = Enum.GetValues(type);
            return values.GetValue(random.Next(values.Length));
        }

        // nullable
        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying != null)
            return GenerateValue(random, underlying);

        // complex type → recursive fuzzing
        if (type.IsClass && type != typeof(string))
        {
            var instance = Activator.CreateInstance(type);
            if (instance != null)           
                PopulateProperties(random, instance);
            
            return instance;
        }

        throw new NotSupportedException($"Type '{type.FullName}' is not supported");
    }

    private static string RandomString(Random random)
    {
        Span<byte> buffer = stackalloc byte[8];
        random.NextBytes(buffer);
        return Convert.ToHexString(buffer);
    }
}