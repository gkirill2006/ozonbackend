function updateOzonAnalytics() {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    
    // Получаем данные из столбца AE для исключения товаров
    const excludeOfferIds = getExcludeOfferIds(sheet);
    
    // Получаем данные обязательных товаров из столбцов AF-AG
    const mandatoryProducts = getMandatoryProducts(sheet);
    
    const params = {
      days: sheet.getRange("B6").getValue(),
      period_analiz: sheet.getRange("B5").getValue(),
      b7: sheet.getRange("B7").getValue(),
      sort_by_qty : sheet.getRange("F8").getValue(),
      f9 : sheet.getRange("F9").getValue(),
      "Api-Key": sheet.getRange("B9").getValue(),
      "client_id": sheet.getRange("B8").getValue(),
      price_min: sheet.getRange("F5").getValue(),
      price_max: sheet.getRange("G5").getValue(),
      sklad_max: sheet.getRange("G6").getValue(),
      f6 : sheet.getRange("F6").getValue(),
      g6 : sheet.getRange("G6").getValue(),
      f7 : sheet.getRange("F7").getValue(),
      f10 : sheet.getRange("F10").getValue(),
      exclude_offer_ids: excludeOfferIds,
      mandatory_products: mandatoryProducts
    };
  
    const payload = {};
    for (const [key, value] of Object.entries(params)) {
      if (value !== "" && value !== null) {
        payload[key] = value;
      }
    }
    // Очищаем старые данные
    sheet.getRange("A16:AC10000").clearContent().clearFormat();
  
    const url = "http://188.68.222.242:9006/api/ozon/analytics/";
    const headers = {
      "Content-Type": "application/json",
    };
  
  
  
    const options = {
      method: "post",
      headers: headers,
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
    };
  
    const response = UrlFetchApp.fetch(url, options);
    const json = JSON.parse(response.getContentText());
  
    
    let row = 16;
  
    for (const cluster of json.clusters) {
      const { cluster_name, cluster_share_percent, cluster_revenue, products } = cluster;
  
      for (const product of products) {
          
        sheet.setRowHeight(row, 100); 
        sheet.getRange(row, 1).setFormula(`=IMAGE("${product.photo}"; 1)`);
        sheet.getRange(row, 2).setValue(product.offer_id);
        sheet.getRange(row, 3).setValue(product.price);
        sheet.getRange(row, 4).setValue(product.barcodes[0]);
        sheet.getRange(row, 5).setValue(product.category);
        sheet.getRange(row, 6).setValue(product.type_name);
        sheet.getRange(row, 7).setFormula(`=HYPERLINK("${product.ozon_link}"; "Ссылка")`);
  
        // Общие заказы (FBS+FBO)			
        sheet.getRange(row, 8).setValue(product.fbs_stock_total_qty);
        sheet.getRange(row, 9).setValue(product.sales_total_fbo_fbs);
        sheet.getRange(row, 10).setValue(product.product_total_revenue_fbo_fbs);
        sheet.getRange(row, 11).setValue(product.avg_daily_sales_fbo_fbs);
        sheet.getRange(row, 12).setValue(product.oborachivaemost);
        
        // Кластер
        sheet.getRange(row, 13).setValue(cluster_name);
        
        //Новые данные
        sheet.getRange(row, 14).setValue(product.average_delivery_time);
        sheet.getRange(row, 15).setValue(product.impact_share);
        sheet.getRange(row, 16).setValue(product.average_delivery_time_item);
        sheet.getRange(row, 17).setValue(product.impact_share_item);
        sheet.getRange(row, 18).setValue(product.recommended_supply_item);
  
  
        sheet.getRange(row, 19).setValue(product.payout_total);
        sheet.getRange(row, 20).setValue(product.sales_qty_cluster);   
        sheet.getRange(row, 21).setValue(product.avg_daily_sales_cluster_rub);  
        sheet.getRange(row, 22).setValue(product.avg_daily_sales_cluster_qty);   
        
        sheet.getRange(row, 23).setValue(product.share_of_total_daily_average);   
        sheet.getRange(row, 24).setValue(product.stock_total_cluster);   
        sheet.getRange(row, 25).setValue(product.need_goods);
        sheet.getRange(row, 26).setValue(product.for_delivery);   
        sheet.getRange(row, 26).setBackground("#FFFF00");
  
  
        row++;
      }
    
    }
        let row_summary = 16;
     for (const summary of json.summary) {
      
      if (summary.total_for_delivery > 0){
        sheet.getRange(row_summary, 27).setBackground("#D9EAD3")
        sheet.getRange(row_summary, 28).setBackground("#D9EAD3")
        sheet.getRange(row_summary, 29).setBackground("#D9EAD3")


      }
      else{
        sheet.getRange(row_summary, 27).setBackground("#F4CCCC")
        sheet.getRange(row_summary, 28).setBackground("#F4CCCC")
        sheet.getRange(row_summary, 29).setBackground("#F4CCCC")


      }
      sheet.getRange(row_summary, 27).setValue(summary.offer_id);    
      sheet.getRange(row_summary, 28).setValue(summary.barcode);    

      sheet.getRange(row_summary, 29).setValue(summary.total_for_delivery); 
      row_summary++      
     }
  
    sheet.getRange(7, 14).setValue(json.average_delivery_time);       
  
  
  
    // Статус обновления
    const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "HH:mm:ss");
    const date = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd.MM.yyyy");
  
    // Статус
    const statusRange = sheet.getRange("J4:M4");
    statusRange.merge();
  
    statusRange.setValue(
    `Статус отчета: Обновление успешно\nTime: ${now}\n${date}\nВремя выполнения запроса на сервере: ${json.execution_time_seconds} сек`
  );
    statusRange.setBackground("#b6d7a8");
  }
  
  /**
   * Получает артикулы товаров для исключения из столбца AF
   * @param {Sheet} sheet - Активный лист
   * @return {Array} Массив артикулов для исключения
   */
  function getExcludeOfferIds(sheet) {
    const excludeOfferIds = [];
    let row = 16;
    
    // Читаем данные из столбца AF начиная с ячейки AF16
    while (true) {
      const cellValue = sheet.getRange(`AF${row}`).getValue();
      
      // Если ячейка пустая, прекращаем чтение
      if (cellValue === "" || cellValue === null || cellValue === undefined) {
        break;
      }
      
      // Преобразуем значение в строку и разделяем по запятой
      const cellString = String(cellValue).trim();
      if (cellString) {
        const items = cellString.split(';').map(item => item.trim()).filter(item => item !== '');
        excludeOfferIds.push(...items);
      }
      
      row++;
    }
    
    // Удаляем дубликаты
    const uniqueOfferIds = [...new Set(excludeOfferIds)];
    
    
    return uniqueOfferIds;
  }
  
    /**
   * Получает обязательные товары из столбцов AG-AH
   * @param {Sheet} sheet - Активный лист
   * @return {Array} Массив объектов с артикулом и количеством
   */
  function getMandatoryProducts(sheet) {
    const mandatoryProducts = [];
    let row = 16;
    
    // Читаем данные из столбцов AG (артикул) и AH (количество) начиная с ячейки AG16
    while (true) {
      const offerId = sheet.getRange(`AG${row}`).getValue();
      const quantity = sheet.getRange(`AH${row}`).getValue();
      
      // Если ячейка с артикулом пустая, прекращаем чтение
      if (offerId === "" || offerId === null || offerId === undefined) {
        break;
      }
      
      // Проверяем, что количество тоже указано
      if (quantity !== "" && quantity !== null && quantity !== undefined && quantity > 0) {
        mandatoryProducts.push({
          offer_id: String(offerId).trim(),
          quantity: parseInt(quantity)
        });
      }
      
      row++;
    }
    
    
    
    return mandatoryProducts;
  }
  