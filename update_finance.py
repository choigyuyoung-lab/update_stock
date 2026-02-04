lets(
	/* 1. 설정: 연결할 속성과 기호 선택 */
	numberProp, prop("EPS"),
	currency, "₩", /* 달러면 "$" 로 변경 */
	width, 15, /* 전체 너비 (글자수) */

	/* 2. 숫자 포맷팅 (천단위 콤마 & 소수점) */
	/* 2-1. 절대값 변환 */
	absVal, abs(numberProp),
	
	/* 2-2. 콤마 찍기 (정규식 활용) */
	/* 정수 부분 */
	integerPart, format(floor(absVal)).replaceAll("(\d)(?=(\d{3})+(?!\d))", "$1,"),
	/* 소수 부분 (필요하면 사용, 없으면 빈칸) */
	decimalPart, if(mod(absVal, 1) > 0, "." + format(absVal).split(".").last().substring(0,2), ""),
	
	/* 최종 숫자 문자열 */
	cleanNum, integerPart + decimalPart,

	/* 3. 공백 채우기 (Spacer) */
	/* 전체 폭에서 심볼과 숫자 길이를 뺀 만큼 공백 반복 */
	spaceCount, max(0, width - currency.length() - cleanNum.length()),
	spacer, repeat(" ", spaceCount),

	/* 4. 최종 조립 및 색상 적용 */
	/* 값이 비어있으면 표시 안 함 */
	if(empty(numberProp), "",
		/* 음수면 빨간색, 양수면 기본색 */
		style(
			/* 포맷: [ 기호 + 공백 + 숫자 ] */
			"[" + currency + spacer + if(numberProp < 0, "-", "") + cleanNum + "]",
			if(numberProp < 0, "red", "default")
		)
	)
)
