import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Switch } from './ui/switch';

export function ShipmentFilters() {
  const [filters, setFilters] = useState({
    planningDays: 28,
    analysisPeriod: 28,
    warehouseWeight: 1,
    priceMin: 1000,
    priceMax: 5000,
    turnoverMin: 10,
    turnoverMax: 90,
    showNoNeed: false,
    sortBy: 'orders',
    specificWeightThreshold: 0.01,
    turnoverFromStock: 5,
  });

  const handleApply = () => {
    console.log('Применение фильтров:', filters);
    // Здесь будет запрос на бэкенд
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Фильтры</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* На сколько дн. планируем */}
          <div className="space-y-2">
            <Label htmlFor="planningDays">На сколько дн. планируем</Label>
            <Input
              id="planningDays"
              type="number"
              value={filters.planningDays}
              onChange={(e) => setFilters({ ...filters, planningDays: Number(e.target.value) })}
            />
          </div>

          {/* Анализируемый период */}
          <div className="space-y-2">
            <Label htmlFor="analysisPeriod">Анализируемый период</Label>
            <Input
              id="analysisPeriod"
              type="number"
              value={filters.analysisPeriod}
              onChange={(e) => setFilters({ ...filters, analysisPeriod: Number(e.target.value) })}
            />
          </div>

          {/* Учитывать вес склада */}
          <div className="space-y-2">
            <Label htmlFor="warehouseWeight">Учитывать вес склада по заказам или рек. Озона</Label>
            <Input
              id="warehouseWeight"
              type="number"
              value={filters.warehouseWeight}
              onChange={(e) => setFilters({ ...filters, warehouseWeight: Number(e.target.value) })}
            />
          </div>

          {/* Цена (min, max) */}
          <div className="space-y-2">
            <Label>Цена (min, max)</Label>
            <div className="flex gap-2">
              <Input
                type="number"
                placeholder="Min"
                value={filters.priceMin}
                onChange={(e) => setFilters({ ...filters, priceMin: Number(e.target.value) })}
              />
              <Input
                type="number"
                placeholder="Max"
                value={filters.priceMax}
                onChange={(e) => setFilters({ ...filters, priceMax: Number(e.target.value) })}
              />
            </div>
          </div>

          {/* Оборачиваемость (min, max) */}
          <div className="space-y-2">
            <Label>Оборачиваемость (min, max)</Label>
            <div className="flex gap-2">
              <Input
                type="number"
                placeholder="Min"
                value={filters.turnoverMin}
                onChange={(e) => setFilters({ ...filters, turnoverMin: Number(e.target.value) })}
              />
              <Input
                type="number"
                placeholder="Max"
                value={filters.turnoverMax}
                onChange={(e) => setFilters({ ...filters, turnoverMax: Number(e.target.value) })}
              />
            </div>
          </div>

          {/* Показывать товары без потребности */}
          <div className="space-y-2">
            <Label htmlFor="showNoNeed">Показывать товары без потребности</Label>
            <div className="flex items-center space-x-2 mt-2">
              <Switch
                id="showNoNeed"
                checked={filters.showNoNeed}
                onCheckedChange={(checked) => setFilters({ ...filters, showNoNeed: checked })}
              />
              <Label htmlFor="showNoNeed" className="cursor-pointer">
                {filters.showNoNeed ? 'Да' : 'Нет'}
              </Label>
            </div>
          </div>

          {/* Сортировка */}
          <div className="space-y-2">
            <Label htmlFor="sortBy">Сортировка по</Label>
            <Select value={filters.sortBy} onValueChange={(value) => setFilters({ ...filters, sortBy: value })}>
              <SelectTrigger id="sortBy">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="orders">Заказ шт.</SelectItem>
                <SelectItem value="revenue">Выручке, руб.</SelectItem>
                <SelectItem value="ozon-rec">Рек. Озона</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Если удельный вес <N, то он = */}
          <div className="space-y-2">
            <Label htmlFor="specificWeightThreshold">Если удельный вес &lt;N, то он =</Label>
            <Input
              id="specificWeightThreshold"
              type="number"
              step="0.01"
              value={filters.specificWeightThreshold}
              onChange={(e) => setFilters({ ...filters, specificWeightThreshold: Number(e.target.value) })}
            />
          </div>

          {/* Учитывать Оборачиваемость от N остатков */}
          <div className="space-y-2">
            <Label htmlFor="turnoverFromStock">Учитывать Оборачиваемость от N остатков</Label>
            <Input
              id="turnoverFromStock"
              type="number"
              value={filters.turnoverFromStock}
              onChange={(e) => setFilters({ ...filters, turnoverFromStock: Number(e.target.value) })}
            />
          </div>
        </div>

        <div className="mt-6">
          <Button onClick={handleApply} className="w-full md:w-auto">
            Применить
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
