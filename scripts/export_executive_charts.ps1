param(
    [Parameter(Mandatory = $true)]
    [string]$InputDir,

    [Parameter(Mandatory = $true)]
    [string]$OutputDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Windows.Forms.DataVisualization
Add-Type -AssemblyName System.Drawing

$manifestPath = Join-Path $InputDir "chart_manifest.json"
if (-not (Test-Path $manifestPath)) {
    throw "Chart manifest not found at $manifestPath"
}

$null = New-Item -ItemType Directory -Path $OutputDir -Force
$manifest = Get-Content $manifestPath -Raw | ConvertFrom-Json

function Set-ChartStyle {
    param(
        [Parameter(Mandatory = $true)]$Chart,
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter(Mandatory = $true)][string]$Subtitle,
        [Parameter(Mandatory = $true)][string]$ChartType,
        [Parameter(Mandatory = $true)][string]$YFormat
    )

    $Chart.Width = 1200
    $Chart.Height = 750
    $Chart.BackColor = [System.Drawing.Color]::White
    $Chart.Palette = [System.Windows.Forms.DataVisualization.Charting.ChartColorPalette]::None
    $Chart.PaletteCustomColors = @(
        [System.Drawing.ColorTranslator]::FromHtml("#1F4E79"),
        [System.Drawing.ColorTranslator]::FromHtml("#4F81BD"),
        [System.Drawing.ColorTranslator]::FromHtml("#7EA6E0"),
        [System.Drawing.ColorTranslator]::FromHtml("#ED7D31"),
        [System.Drawing.ColorTranslator]::FromHtml("#70AD47"),
        [System.Drawing.ColorTranslator]::FromHtml("#A5A5A5")
    )

    $Chart.Titles.Clear()
    $null = $Chart.Titles.Add($Title)
    $Chart.Titles[0].Font = New-Object System.Drawing.Font("Segoe UI", 18, [System.Drawing.FontStyle]::Bold)
    $Chart.Titles[0].ForeColor = [System.Drawing.ColorTranslator]::FromHtml("#1F1F1F")

    $null = $Chart.Titles.Add($Subtitle)
    $Chart.Titles[1].Docking = [System.Windows.Forms.DataVisualization.Charting.Docking]::Top
    $Chart.Titles[1].Font = New-Object System.Drawing.Font("Segoe UI", 10, [System.Drawing.FontStyle]::Regular)
    $Chart.Titles[1].ForeColor = [System.Drawing.ColorTranslator]::FromHtml("#555555")

    $area = $Chart.ChartAreas[0]
    $area.BackColor = [System.Drawing.Color]::White
    $area.AxisX.LabelStyle.Font = New-Object System.Drawing.Font("Segoe UI", 10)
    $area.AxisY.LabelStyle.Font = New-Object System.Drawing.Font("Segoe UI", 10)
    $area.AxisX.Interval = 1
    $area.AxisX.MajorGrid.Enabled = $false
    $area.AxisY.MajorGrid.LineColor = [System.Drawing.ColorTranslator]::FromHtml("#D9E1F2")
    $area.AxisY.MajorGrid.LineDashStyle = [System.Windows.Forms.DataVisualization.Charting.ChartDashStyle]::Dash
    $area.AxisX.LineColor = [System.Drawing.ColorTranslator]::FromHtml("#7F7F7F")
    $area.AxisY.LineColor = [System.Drawing.ColorTranslator]::FromHtml("#7F7F7F")

    if ($YFormat -eq "percent") {
        $area.AxisY.LabelStyle.Format = "P0"
        $area.AxisY.Maximum = 1
        $area.AxisY.Minimum = 0
        $area.AxisY.Interval = 0.1
    } elseif ($YFormat -eq "decimal") {
        $area.AxisY.LabelStyle.Format = "0.00"
    } else {
        $area.AxisY.LabelStyle.Format = "#,##0"
    }

    $legend = $Chart.Legends[0]
    $legend.Docking = [System.Windows.Forms.DataVisualization.Charting.Docking]::Bottom
    $legend.Font = New-Object System.Drawing.Font("Segoe UI", 10)

    foreach ($series in $Chart.Series) {
        $series.Font = New-Object System.Drawing.Font("Segoe UI", 9)
        $series.IsValueShownAsLabel = $false
        if ($YFormat -eq "percent") {
            $series.LabelFormat = "P0"
        } elseif ($YFormat -eq "decimal") {
            $series.LabelFormat = "0.00"
        } else {
            $series.LabelFormat = "#,##0"
        }

        if ($ChartType -eq "stacked_bar") {
            $series.ChartType = [System.Windows.Forms.DataVisualization.Charting.SeriesChartType]::StackedBar
        } elseif ($ChartType -eq "line") {
            $series.ChartType = [System.Windows.Forms.DataVisualization.Charting.SeriesChartType]::Line
            $series.BorderWidth = 3
            $series.MarkerStyle = [System.Windows.Forms.DataVisualization.Charting.MarkerStyle]::Circle
            $series.MarkerSize = 7
        } else {
            $series.ChartType = [System.Windows.Forms.DataVisualization.Charting.SeriesChartType]::Bar
        }
    }
}

foreach ($item in $manifest) {
    $csvPath = $item.csv_path
    if (-not (Test-Path $csvPath)) {
        continue
    }

    $rows = Import-Csv -Path $csvPath
    if (-not $rows) {
        continue
    }

    $chart = New-Object System.Windows.Forms.DataVisualization.Charting.Chart
    $null = $chart.ChartAreas.Add("Main")
    $null = $chart.Legends.Add("Legend")

    foreach ($field in $item.series_fields) {
        $series = New-Object System.Windows.Forms.DataVisualization.Charting.Series($field)
        foreach ($row in $rows) {
            $xValue = [string]$row.($item.x_field)
            $rawValue = $row.$field
            if ([string]::IsNullOrWhiteSpace($rawValue)) {
                continue
            }
            $value = 0.0
            [void][double]::TryParse($rawValue, [ref]$value)
            $null = $series.Points.AddXY($xValue, $value)
        }
        $null = $chart.Series.Add($series)
    }

    Set-ChartStyle -Chart $chart -Title $item.title -Subtitle $item.subtitle -ChartType $item.chart_type -YFormat $item.y_format
    $pngPath = Join-Path $OutputDir ([System.IO.Path]::GetFileName($item.png_path))
    $chart.SaveImage($pngPath, [System.Windows.Forms.DataVisualization.Charting.ChartImageFormat]::Png)
    $chart.Dispose()
}

Write-Output "Exported executive chart PNGs to $OutputDir"
