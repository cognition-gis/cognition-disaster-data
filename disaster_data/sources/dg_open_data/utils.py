

def items_from_imagery_table(table):
    out_list = []
    for row in table.xpath('.//tbody/tr'):
        date = row.xpath('.//td/p/text()').get()
        assets = [x for x in row.xpath('.//td/a/@href').getall() if x.endswith('.tif')]
        for asset in assets:
            partial_item = {
                'properties': {
                    'datetime': date
                },
                'asset': {
                    'data': {
                        'href': asset
                    }
                }
            }
            out_list.append(partial_item)
    return out_list