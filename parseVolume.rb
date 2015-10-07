require 'pg'
conn = PG::Connection.open(:dbname => 'SmartStats_development')
conn.prepare('insertVolume', 'insert into traffics ( year, month, day, hour, minute, volume)
values ($1, $2, $3, $4, $5, $6);')
File.open("file", "r") do |f|
f.each_line do |line|
    date = line[/(\d+)\s+(\d+)/, 1]
    volume = line[/(\d+)\s+(\d+)/, 2]
res = conn.exec_prepared('insertVolume', [date[0,4], date[4,2], date[6,2], date[8,2], date[10,2], volume.to_i]) 
 end
end
