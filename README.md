# pagedeleted statistics report
Wikidata bot that summarizes the content of data items that have lost all sitelinks.

Data items that have lost all sitelinks may have lost notability in Wikidata. They are reported by another bot (by @Pascalco/User:Pasleim) on [pagedeleted archives](https://www.wikidata.org/wiki/User:Pasleim/Items_for_deletion/Page_deleted). Since thousands of items have backlogged over time, some overview is necessary in order to aid processing. This bot iterates over the current pagedeleted list and all its archives and tries to extract which information is to be found within the listed items.

The report currently appears at [pagedeleted_stats](https://www.wikidata.org/wiki/User:MisterSynergy/sysop/pagedeleted_stats).

## Technical requirements
The bot is currently scheduled to run weekly on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) from within the `msynbot` tool account. It depends on the [shared pywikibot files](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Pywikibot#Using_the_shared_Pywikibot_files_(recommended_setup)) and is running in a Kubernetes environment using Python 3.11.2.