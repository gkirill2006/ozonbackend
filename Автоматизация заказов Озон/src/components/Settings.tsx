import { Card, CardContent, CardHeader, CardTitle } from './ui/card';

export function Settings() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Настройки</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-gray-500">Раздел "Настройки" в разработке</p>
      </CardContent>
    </Card>
  );
}
