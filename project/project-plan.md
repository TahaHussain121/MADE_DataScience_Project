# Project Plan

## Title
<!-- Give your project a short title. -->
The Impact of H1B Hiring on Local Wages in Washington: A Microsoft Case Study

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
How Does Microsoft's H1B Hiring Influence Wage Levels for Local Tech Professionals in Washington State?



## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
The objective of this study is to analyze the wage differences between H1B workers employed by Microsoft and the local tech workforce in Washington State. Using datasets such as the H1B Labor Condition Application (LCA) disclosures and the Occupational Employment and Wage Statistics (OEWS), this project investigates how H1B hiring practices correlate with local wage trends.

The analysis involves integrating and transforming datasets to align with the study's scope, filtering for Microsoft-specific roles, and comparing H1B wages to local averages for equivalent job codes. The study will focus on temporal consistency to mitigate discrepancies between datasets collected at different times.

To ensure robust findings, control variables such as role-specific demand and industry employment levels will be considered. The project’s findings will be visualized through plots that compare wages and highlight disparities between H1B and local workers. These insights aim to inform policymakers about the economic impact of H1B hiring practices and guide decisions on workforce management and wage policies.




## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: H1B Labor Condition Application Dataset (LCA)
* Metadata URL: https://www.dol.gov
* Data URL: https://afdc.energy.gov/data_download
* Data Type: Excel

Provides information on job roles, employers, wages, SOC codes, and locations for H1B workers.


### Datasource1:  Occupational Employment and Wage Statistics (OEWS)
* Metadata URL: https://esd.wa.gov/
* Data URL: https://esd.wa.gov/media/2861
* Data Type: Excel

Contains aggregated wage data for various job roles in Washington State, segmented by SOC codes and occupational titles.


## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Integrate data from previous years [#3][i3]
2. Refactor code and add sh file [#4][i4]
3. automated testing [#5][i5]

[i3]: https://github.com/TahaHussain121/MADE_DataScience_Project/issues/3
[i4]: https://github.com/TahaHussain121/MADE_DataScience_Project/issues/4
[i5]: https://github.com/TahaHussain121/MADE_DataScience_Project/issues/5
