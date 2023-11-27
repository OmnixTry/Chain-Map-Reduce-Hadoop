const string originalFile = "D:\\Programming\\Big Data\\Hadoop Docker Installation\\problem-solving\\weather_data.csv";
const string temperatureColumnName = "GB_temperature";
var lines = await File.ReadAllLinesAsync(originalFile);

var header = lines[0].Split(',').ToList();
int temperatureIndex = header.IndexOf(temperatureColumnName);

List<string> finalLines = new List<string>();

for (int i = 1; i < lines.Length; i++)
{

	var line = lines[i].Split(',');
	var date = DateTime.Parse(line[0]);
	if (date.Hour == 13)
	{
		finalLines.Add($"{date.ToString("yyyy-MM-dd")},{line[temperatureIndex]}");
	}
}

File.WriteAllLines("Result.csv", finalLines);